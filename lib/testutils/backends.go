// Copyright (c) 2016-2017 Tigera, Inc. All rights reserved.

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

package testutils

import (
	"errors"
	"fmt"
	"log"
	"os/exec"

	. "github.com/onsi/ginkgo/extensions/table"
	"github.com/projectcalico/libcalico-go/lib/api"
	"github.com/projectcalico/libcalico-go/lib/client"
	"os"
)

// CleanBackend is a utility function to wipe clean "/calico" recursively from backend.
func CleanBackend(config *api.CalicoAPIConfig) {
	var err error

	log.Println(fmt.Sprintf("Cleaning datastore: %v", config.Spec.DatastoreType))

	switch config.Spec.DatastoreType {
	case api.EtcdV2:
		err = exec.Command("etcdctl", "rm", "/calico", "--recursive").Run()
	case api.Consul:
		err = exec.Command("consul", "kv", "delete", "-recurse", "calico").Run()
	case api.Kubernetes:
	default:
		err = errors.New(fmt.Sprintf("Unknown datastore type: %v", config.Spec.DatastoreType))
	}

	if err != nil {
		log.Println(err)
	}
}

// DumpBackend prints out a recursive dump of the contents of backend.
func DumpBackend(config *api.CalicoAPIConfig) error {
	var output []byte
	var err error

	log.Println(fmt.Sprintf("Dumping datastore: %v", config.Spec.DatastoreType))

	switch config.Spec.DatastoreType {
	case api.EtcdV2:
		output, err = exec.Command("curl", "http://127.0.0.1:2379/v2/keys?recursive=true").Output()
	case api.Consul:
		output, err = exec.Command("curl", "http://127.0.0.1:8500/v1/kv/calico?recurse=").Output()
	case api.Kubernetes:
	default:
		err = errors.New(fmt.Sprintf("Unknown datastore type: %v", config.Spec.DatastoreType))
	}

	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			log.Printf("Dump backend return error: %s, %v", string(ee.Stderr), *ee.ProcessState)
		} else {
			log.Println(err)
		}
	} else {
		log.Println(string(output))
	}

	return err
}

func getConfigFileNames() []string {
	envFromConfig := os.Getenv("CONFIG_PATH")

	if envFromConfig != "" {
		return []string{
			envFromConfig,
		}
	}

	// this is bad, I know. Suggestions are welcome
	return []string{
		"../testutils/config/etcdv2.yaml",
		"../testutils/config/consul.yaml",
	}
}

func GetEmptyEntriesWithConfigs() []TableEntry {
	return EnhanceWithConfigs(Entry(""))
}

func EnhanceWithConfigs(entries ...TableEntry) []TableEntry {
	result := []TableEntry{}

	for _, configFileName := range getConfigFileNames() {
		config, err := client.LoadClientConfig(configFileName)
		if err != nil {
			log.Println(err)
		}

		for _, entry := range entries {
			var description string
			if entry.Description == "" {
				description = fmt.Sprintf("%s", config.Spec.DatastoreType)
			} else {
				description = fmt.Sprintf("%s: %s", config.Spec.DatastoreType, entry.Description)
			}
			result = append(result, TableEntry{
				Description: description,
				Focused:     entry.Focused,
				Parameters:  append(entry.Parameters, config),
				Pending:     entry.Pending,
			})
		}
	}
	return result
}
