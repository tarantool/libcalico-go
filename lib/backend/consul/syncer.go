// Copyright (c) 2016 Tigera, Inc. All rights reserved.
//
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
	log "github.com/Sirupsen/logrus"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	"time"
)

// Consul syncer uses blocking call to consul replica to list all keys
// that have version greater than remembered. Unfortunately, consul will
// send us full snapshot, so we should manage to create our own diffs
//
// Architecture
//
// Syncer runs watcher goroutine which polls consul and report diffs to api
func newSyncer(client *consulapi.Client, callbacks api.SyncerCallbacks) *consulSyncer {
	return &consulSyncer{
		callbacks: callbacks,
		client:    client,
	}
}

type consulSyncer struct {
	callbacks api.SyncerCallbacks
	client    *consulapi.Client
	OneShot   bool
}

func (syn *consulSyncer) Start() {

}

func (syn *consulSyncer) watchConsul() {
	log.Info("consul watch thread started.")

	for {
		syn.callbacks.OnStatusUpdated(api.WaitForDatastore)

		// Do a non-recursive get on the Ready flag to find out the
		// current consul index.  We'll trigger a snapshot/start polling from that.
		_, meta, err := syn.client.KV().Get("/calico/v1/Ready", nil)
		if err != nil {
			log.WithError(err).Warn("Failed to get Ready key from consul")
			time.Sleep(1 * time.Second)
			continue
		}

		clusterIndex := meta.LastIndex
		log.WithField("index", clusterIndex).Info("Polled consul for initial watch index.")

		results, meta, err := syn.client.KV().List(
			"/calico/v1/",
			&consulapi.QueryOptions{WaitIndex: clusterIndex})

		if err != nil {
			log.WithError(err).Warn("Failed to get snapshot from consul")
			continue
		}

		syn.callbacks.OnStatusUpdated(api.ResyncInProgress)

		initialUpdates := make([]api.Update, len(results))
		state := &ClusterState{}
		for i, x := range results {
			key := model.KeyFromDefaultPath(x.Key)
			value, err := model.ParseValue(key, x.Value)
			updateType := api.UpdateTypeKVNew
			if err != nil {
				log.WithField("key", key).Warn("Can't parse value at key.")
				updateType = api.UpdateTypeKVUnknown
				value = nil
			}

			initialUpdates[i] = api.Update{
				UpdateType: updateType,
				KVPair: model.KVPair{
					Key:      key,
					Revision: x.ModifyIndex,
					Value:    value,
				},
			}

			state.Add(key)
		}

		syn.callbacks.OnUpdates(initialUpdates)
		syn.callbacks.OnStatusUpdated(api.InSync)

		// second sync will be run in cycle until we have some consul error
	watchLoop:
		for {
			err = repeatableSync(syn, state, clusterIndex)
			if err != nil {
				log.WithError(err).Warn("Failed to get snapshot from consul")
				break watchLoop
			}
		}
	}
}
func repeatableSync(syn *consulSyncer, state *ClusterState, clusterIndex uint64) error {
	results, meta, err := syn.client.KV().List(
		"/calico/v1/",
		&consulapi.QueryOptions{WaitIndex: clusterIndex})

	if err != nil {
		return err
	}

	syn.callbacks.OnStatusUpdated(api.ResyncInProgress)
	updates := make([]api.Update, len(results))
	tombstones := map[model.Key]bool{}
	for i, x := range results {
		key := model.KeyFromDefaultPath(x.Key)
		value, err := model.ParseValue(key, x.Value)
		updateType := api.UpdateTypeKVNew

		if tombstones[key] {
			delete(tombstones, key)
			updateType = api.UpdateTypeKVUpdated
		}

		if err != nil {
			log.WithField("key", key).Warn("Can't parse value at key.")
			updateType = api.UpdateTypeKVUnknown
			value = nil
		}

		updates[i] = api.Update{
			UpdateType: updateType,
			KVPair: model.KVPair{
				Key:      key,
				Revision: x.ModifyIndex,
				Value:    value,
			},
		}

		state.Add(key)
	}
	for x := range tombstones {
		updates = append(updates, api.Update{
			UpdateType: api.UpdateTypeKVDeleted,
			KVPair: model.KVPair{
				Key:      x,
				Revision: meta.LastIndex,
				Value:    nil,
			},
		})
		state.Remove(x)
	}

	syn.callbacks.OnUpdates(updates)
	syn.callbacks.OnStatusUpdated(api.InSync)
	return nil
}
