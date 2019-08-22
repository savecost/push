//  Copyright © 2018 Sunface <CTO@188.com>
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
// limitations under the License1.
package meq

import (
	"errors"
	"fmt"

	"github.com/imdevlab/flap/internal/pkg/message"
	"github.com/imdevlab/flap/internal/pkg/network/mqtt"
)

func (c *Connection) ReduceCount(topic []byte, count int) error {
	sub, ok := c.subs[string(topic)]
	if !ok {
		// subscribe to the topic failed
		return errors.New("subscribe topic failed,please re-subscribe")
	}

	if !sub.acked {
		return errors.New("subscribe topic failed,please re-subscribe")
	}

	if count <= 0 && count != message.REDUCE_ALL_COUNT {
		return fmt.Errorf("ack count cant below 0,except count == message.ACK_ALL_COUNT")
	}

	if count > message.MAX_PULL_COUNT {
		return fmt.Errorf("ack count cant greater than message.MAX_PULL_COUNT")
	}

	msg := mqtt.Publish{
		Header: &mqtt.StaticHeader{
			QOS: 0,
		},
		Topic:   topic,
		Payload: message.PackReduceCount(count),
	}
	msg.EncodeTo(c.conn)

	return nil
}
