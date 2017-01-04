// Copyright (c) 2017 Tigera, Inc. All rights reserved.

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

	"github.com/projectcalico/libcalico-go/lib/api"
	"github.com/projectcalico/libcalico-go/lib/client"
)

// CleanBackend is a utility function to wipe clean "/calico" recursively from backend.
func CleanBackend(configFileName string) {
	config, err := client.LoadClientConfig(configFileName)
	switch config.Spec.DatastoreType {
	case api.EtcdV2:
		err = exec.Command("etcdctl", "rm", "/calico", "--recursive").Run()
	case api.Kubernetes:
	case api.Consul:
	default:
		err = errors.New(fmt.Sprintf("Unknown datastore type: %v", config.Spec.DatastoreType))
	}

	if err != nil {
		log.Println(err)
	}
}

// DumpBackend prints out a recursive dump of the contents of backend.
func DumpBackend(configFileName string) {
	config, err := client.LoadClientConfig(configFileName)
	var output []byte
	switch config.Spec.DatastoreType {
	case api.EtcdV2:
		output, err = exec.Command("curl", "http://127.0.0.1:2379/v2/keys?recursive=true").Output()
	case api.Kubernetes:
	case api.Consul:
	default:
		err = errors.New(fmt.Sprintf("Unknown datastore type: %v", config.Spec.DatastoreType))
	}

	if err != nil {
		log.Println(err)
	} else {
		log.Println(string(output))
	}
}
