package consul

import "github.com/projectcalico/libcalico-go/lib/backend/model"

type ClusterState struct {
	state map[model.Key]bool
}

func (cs *ClusterState) Add(key model.Key) {
	cs.state[key] = true
}

func (cs *ClusterState) Remove(key model.Key) {
	delete(cs.state, key)
}

func (cs *ClusterState) Contains(key model.Key) bool {
	return cs.state[key]
}
