package cluster

import (
	"bytes"
	"encoding/gob"

	"github.com/weaveworks/mesh"
)

// Sub represent a client subscription
type Sub struct {
	Peer mesh.PeerName
}

// Subs is the set of Sub
// first uint64: topic id
// second uint 64: client id
type Subs map[uint32]map[uint64]*Sub

func (ss Subs) Encode() [][]byte {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(ss)
	if err != nil {
		panic(err)
	}

	return [][]byte{buf.Bytes()}
}

func (ss Subs) Merge(other mesh.GossipData) (complete mesh.GossipData) {
	otherSubs := other.(Subs)
	for otid, osubs := range otherSubs {
		subs, ok := ss[otid]
		if !ok {
			// otid对应的subs不存在，直接赋值
			ss[otid] = osubs
		} else {
			// 存在，进行合并
			for ocid, osub := range osubs {
				_, ok := subs[ocid]
				if !ok {
					// client不存在，赋值
					subs[ocid] = osub
				}
			}
		}
	}

	return
}

func (ss Subs) removePeer(peer mesh.PeerName) {
	for _, subs := range ss {
		for cid, sub := range subs {
			if sub.Peer == peer {
				delete(subs, cid)
			}
		}
	}
}
