package cluster

import (
	"bytes"
	"encoding/gob"

	"github.com/weaveworks/mesh"

	"go.uber.org/zap"

	"github.com/imdevlab/g"
)

/*
Here defines the events happened in cluster
*/

const (
	TypeSubscribe   = 1
	TypeUnsubscribe = 2
)

// When a node comes online, it will broadcast the online message to all peers
type SubscribeEvent struct {
	Type int8
	Tid  uint32
	Cid  uint64
}

func (m SubscribeEvent) Encode() [][]byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		g.L.Info("encode onlineMessage error", zap.Error(err))
	}

	return [][]byte{buf.Bytes()}
}

func (m SubscribeEvent) Merge(new mesh.GossipData) (complete mesh.GossipData) {
	return
}
