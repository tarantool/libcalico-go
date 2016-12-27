// Copyright (c) 2016 Tigera, Inc. All rights reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consul

import (
	goerrors "errors"

	"time"

	log "github.com/Sirupsen/logrus"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	"github.com/projectcalico/libcalico-go/lib/errors"
)

var (
	watchTimeout = 30 * time.Second
)

type ConsulConfig struct {
	ConsulScheme     string `json:"consulScheme" envconfig:"consul_SCHEME" default:"http"`
	ConsulAddress    string `json:"consulAddress" envconfig:"consul_ADDRESS" default:"127.0.0.1:2379"`
	ConsulUsername   string `json:"consulUsername" envconfig:"consul_USERNAME"`
	ConsulPassword   string `json:"consulPassword" envconfig:"consul_PASSWORD"`
	ConsulToken      string `json:"consulToken" envconfig:"consul_TOKEN"`
	ConsulDatacenter string `json:"consulDatacenter" envconfig:"consul_DATACENTER"`
}

type ClientWrapper struct {
	Client *consulapi.Client
}

type setOperationKind int

const (
	create setOperationKind = iota
	replace
	update
)

type setOptions struct {
	Kind  setOperationKind
	Index uint64
}

func NewConsulClient(config *ConsulConfig) (*ClientWrapper, error) {
	auth := consulapi.HttpBasicAuth{
		Password: config.ConsulPassword,
		Username: config.ConsulUsername,
	}
	if len(config.ConsulAddress) == 0 {
		return nil, goerrors.New("no consul address provided")
	}

	cfg := consulapi.Config{
		Address:    config.ConsulAddress,
		Scheme:     config.ConsulScheme,
		Datacenter: config.ConsulDatacenter,
		Token:      config.ConsulToken,
		HttpAuth:   &auth,
		WaitTime:   watchTimeout,
	}

	client, err := consulapi.NewClient(&cfg)
	if err != nil {
		return nil, err
	}
	return &ClientWrapper{Client: client}, nil
}

// EnsureInitialized makes sure that the etcd data is initialized for use by
// Calico.
func (c *ClientWrapper) EnsureInitialized() error {
	// Make sure the Ready flag is initialized in the datastore
	kv := &model.KVPair{
		Key:   model.ReadyFlagKey{},
		Value: true,
	}
	if _, err := c.Create(kv); err == nil {
		log.Info("Ready flag is now set")
	} else {
		if _, ok := err.(errors.ErrorResourceAlreadyExists); !ok {
			log.WithError(err).Warn("Failed to set ready flag")
			return err
		}
		log.Info("Ready flag is already set")
	}

	return nil
}

func (c *ClientWrapper) Syncer(callbacks api.SyncerCallbacks) api.Syncer {
	return newSyncer(c.Client, callbacks)
}

// Create an entry in the datastore.  This errors if the entry already exists.
func (c *ClientWrapper) Create(d *model.KVPair) (*model.KVPair, error) {
	return c.set(d, &setOptions{Kind: create})
}

// Update an existing entry in the datastore.  This errors if the entry does
// not exist.
func (c *ClientWrapper) Update(d *model.KVPair) (*model.KVPair, error) {
	// If the request includes a revision, set it as the etcd previous index.
	options := setOptions{Kind: update}
	if d.Revision != nil {
		options.Index = d.Revision.(uint64)
		log.Debugf("Performing CAS against consulapi index: %v\n", options.Index)
	}

	return c.set(d, &options)
}

// Set an existing entry in the datastore.  This ignores whether an entry already
// exists.
func (c *ClientWrapper) Apply(d *model.KVPair) (*model.KVPair, error) {
	return c.set(d, &setOptions{Kind: replace})
}

// Delete an entry in the datastore.  This errors if the entry does not exists.
func (c *ClientWrapper) Delete(d *model.KVPair) error {
	key, err := model.KeyToDefaultDeletePath(d.Key)
	if err != nil {
		return err
	}
	deleteOp := &consulapi.KVTxnOp{Key: key}
	if d.Revision != nil {
		deleteOp.Index = d.Revision.(uint64)
	}
	log.Debugf("Delete Key: %s", key)

	kv := c.Client.KV()

	readyKey, err := model.KeyToDefaultPath(model.ReadyFlagKey{})
	if err != nil {
		return err
	}
	updateReadyOp := &consulapi.KVTxnOp{Key:readyKey}
	ok, response, _, err := kv.Txn(consulapi.KVTxnOps{deleteOp, updateReadyOp}, nil)
	if err != nil {
		return convertConsulError(err, d.Key)
	}

	if !ok {
		return goerrors.New("Delete fails, transaction rollbacked")
	}

	if len(response.Errors) > 0 {
		return createError(response.Errors)
	}

	// If there are parents to be deleted, delete these as well provided there
	// are no more children.
	parents, err := model.KeyToDefaultDeleteParentPaths(d.Key)
	if err != nil {
		return err
	}

	for _, parent := range parents {
		log.Debugf("Delete empty Key: %s", parent)
		_, err2 := kv.Delete(parent, nil)
		if err2 != nil {
			log.Debugf("Unable to delete parent: %s", err2)
			break
		}
	}

	return convertConsulError(err, d.Key)
}

// Get an entry from the datastore.  This errors if the entry does not exist.
func (c *ClientWrapper) Get(k model.Key) (*model.KVPair, error) {
	key, err := model.KeyToDefaultPath(k)
	if err != nil {
		return nil, err
	}
	log.Debugf("Get Key: %s", key)
	r, _, err := c.Client.KV().Get(key, &consulapi.QueryOptions{RequireConsistent: true})
	if err != nil {
		return nil, convertConsulError(err, k)
	}

	v, err := model.ParseValue(k, r.Value)
	if err != nil {
		return nil, err
	}

	return &model.KVPair{Key: k, Value: v, Revision: r.ModifyIndex}, nil
}

// List entries in the datastore.  This may return an empty list of there are
// no entries matching the request in the ListInterface.
func (c *ClientWrapper) List(l model.ListInterface) ([]*model.KVPair, error) {
	// We need to handle the listing of HostMetadata separately for two reasons:
	// -  older deployments may not have a Metadata, and instead we need to enumerate
	//    based on existence of the directory
	// -  it is not sensible to enumerate all of the endpoints, so better to enumerate
	//    the host directories and then attempt to get the metadata.
	switch lt := l.(type) {
	case model.HostMetadataListOptions:
		return c.listHostMetadata(lt)
	default:
		return c.defaultList(l)
	}
}

// defaultList provides the default list processing.
func (c *ClientWrapper) defaultList(l model.ListInterface) ([]*model.KVPair, error) {
	// To list entries, we enumerate from the common root based on the supplied
	// IDs, and then filter the results.
	key := model.ListOptionsToDefaultPathRoot(l)
	log.Debugf("List Key: %s", key)
	kv := c.Client.KV()

	pairs, _, err := kv.List(key, nil)

	if err != nil {
		// If the root key does not exist - that's fine, return no list entries.
		err = convertConsulError(err, nil)
		switch err.(type) {
		case errors.ErrorResourceDoesNotExist:
			return []*model.KVPair{}, nil
		default:
			return nil, err
		}
	}

	list := filterConsulList(&pairs, l)

	switch t := l.(type) {
	case model.ProfileListOptions:
		return t.ListConvert(list), nil
	}
	return list, nil
}

// Process a node returned from a list to filter results based on the List type and to
// compile and return the required results.
func filterConsulList(pairs *consulapi.KVPairs, l model.ListInterface) []*model.KVPair {
	kvs := []*model.KVPair{}

	for _, x := range *pairs {
		key := l.KeyFromDefaultPath(x.Key)
		if key == nil {
			continue
		}

		if v, err := model.ParseValue(key, x.Value); err == nil {
			kv := &model.KVPair{Key: key, Value: v, Revision: x.ModifyIndex}
			kvs = append(kvs, kv)
		}
	}
	log.Debugf("Returning: %#v", kvs)
	return kvs
}

// Set an existing entry in the datastore.  This ignores whether an entry already
// exists.
func (c *ClientWrapper) set(d *model.KVPair, options *setOptions) (*model.KVPair, error) {
	logCxt := log.WithFields(log.Fields{
		"key":   d.Key,
		"value": d.Value,
		"ttl":   d.TTL,
		"rev":   d.Revision,
	})
	key, err := model.KeyToDefaultPath(d.Key)
	if err != nil {
		logCxt.WithError(err).Error("Failed to convert key to path")
		return nil, err
	}

	bytes, err := model.SerializeValue(d)
	if err != nil {
		logCxt.WithError(err).Error("Failed to serialize value")
		return nil, err
	}

	if d.TTL != 0 {
		// Implement it via sessions & TTL
		return nil, goerrors.New("Consul does not support TTL")
	}
	logCxt.WithField("options", options).Debug("Setting KV in consulapi")

	ops := consulapi.KVTxnOps{}
	switch options.Kind {
	case create:
		ops = append(ops, &consulapi.KVTxnOp{
			Key:   key,
			Verb:  consulapi.KVCAS,
			Value: bytes,
			Index: 0,
		})
		break
	case update:
		ops = append(ops, &consulapi.KVTxnOp{
			Key:   key,
			Verb:  consulapi.KVCAS,
			Value: bytes,
			Index: options.Index,
		})
		break
	case replace:
		ops = append(ops, &consulapi.KVTxnOp{
			Key:   key,
			Verb:  consulapi.KVSet,
			Value: bytes,
		})
		break
	default:
		log.WithField("kind", options.Kind).Error("Unsupported set operation")
		return nil, goerrors.New("Unsupported set operation")
	}

	ops = append(ops, &consulapi.KVTxnOp{
		Key:  key,
		Verb: consulapi.KVGet,
	})

	ok, response, _, err := c.Client.KV().Txn(ops, nil)

	if err != nil {
		// Log at debug because we don't know how serious this is.
		// Caller should log if it's actually a problem.
		logCxt.WithError(err).Debug("Set failed")
		return nil, convertConsulError(err, d.Key)
	}

	if !ok {
		// this means that transaction was rolled back.
		// Log at debug because we don't know how serious this is.
		// Caller should log if it's actually a problem.
		err = createError(response.Errors)
		logCxt.WithError(err).Debug("Set failed")
		return nil, err
	}

	// Datastore object will be identical except for the modified index.
	logCxt.WithField("newRev", response.Results[1].ModifyIndex).Debug("Set succeeded")

	d.Revision = response.Results[1].ModifyIndex
	d.Value, err = model.ParseValue(d.Key, response.Results[1].Value)
	if err != nil {
		// Log at debug because we don't know how serious this is.
		// Caller should log if it's actually a problem.
		logCxt.WithError(err).Debug("Can't parse value returned from consulapi")
		return nil, convertConsulError(err, d.Key)
	}

	return d, nil
}
func createError(errors consulapi.TxnErrors) error {
	if errors == nil {
		return nil
	}

	return goerrors.New("some errors in consul")
}

func convertConsulError(err error, key model.Key) error {
	if err == nil {
		log.Debug("Command completed without error")
		return nil
	}

	switch err.(type) {
	default:
		log.Infof("Unhandled error: %v", err)
		return errors.ErrorDatastoreError{Err: err, Identifier: key}
	}
}

func (c *ClientWrapper) listHostMetadata(l model.HostMetadataListOptions) ([]*model.KVPair, error) {
	// If the hostname is specified then just attempt to get the host,
	// returning an empty string if it does not exist.
	if l.Hostname != "" {
		log.Debug("Listing host metadata with exact key")
		hmk := model.HostMetadataKey{
			Hostname: l.Hostname,
		}

		kv, err := c.Get(hmk)
		if err == nil {
			return []*model.KVPair{kv}, nil
		}

		err = convertConsulError(err, nil)
		switch err.(type) {
		case errors.ErrorResourceDoesNotExist:
			return []*model.KVPair{}, nil
		default:
			return nil, err
		}
	}

	// No hostname specified, so enumerate the directories directly under
	// the host tree, return no entries if the host directory does not exist.
	log.Debug("Listing all host metadatas")
	key := "/calico/v1/host"
	kv := c.Client.KV()
	results, _, err := kv.List(key, nil)
	if err != nil {
		// If the root key does not exist - that's fine, return no list entries.
		log.WithError(err).Info("Error enumerating host directories")
		err = convertConsulError(err, nil)
		switch err.(type) {
		case errors.ErrorResourceDoesNotExist:
			return []*model.KVPair{}, nil
		default:
			return nil, err
		}
	}

	// TODO:  Since the host metadata is currently empty, we don't need
	// to perform an additional get here, but in the future when the metadata
	// may contain fields, we would need to perform a get.
	log.Debug("Parse host directories.")
	kvs := []*model.KVPair{}
	for _, n := range results {
		k := l.KeyFromDefaultPath(n.Key + "/metadata")
		if k != nil {
			kvs = append(kvs, &model.KVPair{
				Key:   k,
				Value: &model.HostMetadata{},
			})
		}
	}
	return kvs, nil
}
