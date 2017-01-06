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

package backend_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/projectcalico/libcalico-go/lib/backend/model"

	"github.com/projectcalico/libcalico-go/lib/api"
	"github.com/projectcalico/libcalico-go/lib/backend"
	"github.com/projectcalico/libcalico-go/lib/testutils"
)

var _ = Describe("Backend tests", func() {
	DescribeTable("etcd GET/List Revision values",
		func(config *api.CalicoAPIConfig) {
			testutils.CleanBackend(config)

			client, _ := backend.NewRawClient(*config)

			Context("CREATE a Block", func() {
				block := &model.KVPair{
					Key: model.BlockKey{
						CIDR: testutils.MustParseCIDR("10.0.0.0/26"),
					},
					Value: model.AllocationBlock{
						CIDR: testutils.MustParseCIDR("10.0.0.0/26"),
					},
				}

				_, cErr := client.Create(block)

				Expect(cErr).NotTo(HaveOccurred(), "should succeed without an error")
			})

			Context("GET BlockKey", func() {
				key := model.BlockKey{
					CIDR: testutils.MustParseCIDR("10.0.0.0/26"),
				}
				b, bErr := client.Get(key)
				Expect(bErr).NotTo(HaveOccurred(), "Revision field should not be nil")
				Expect(b.Revision).NotTo(BeNil(), "Revision field should not be nil")
			})

			Context("LIST BlockKey", func() {
				blockListOpt := model.BlockListOptions{
					IPVersion: 4,
				}
				bl, blErr := client.List(blockListOpt)
				for _, blv := range bl {
					Expect(blErr).NotTo(HaveOccurred(), "Revision field should not be nil")
					Expect(blv.Revision).NotTo(BeNil(), "Revision field should not be nil")
				}
			})
		},
		testutils.GetEmptyEntriesWithConfigs()...,
	)
})
