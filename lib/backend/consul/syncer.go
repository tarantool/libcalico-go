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
	hashicorpConsul "github.com/hashicorp/consul/api"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
)

// defaultEtcdClusterID is default value that an etcd cluster uses if it
// hasn't been bootstrapped with an explicit value.  We warn if we detect that
// case because it implies that the cluster hasn't been properly bootstrapped
// for production.
func newSyncer(client *hashicorpConsul.Client, callbacks api.SyncerCallbacks) *Syncer {
	return nil
}

type Syncer struct {
	callbacks api.SyncerCallbacks
	client    *hashicorpConsul.Client
	OneShot   bool
}

func (syn *Syncer) Start() {
}
