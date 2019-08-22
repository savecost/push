//  Copyright © 2018 Sunface <CTO@188.com>

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package message

import (
	"errors"

	"github.com/imdevlab/g/utils"
)

/* A topic must be like these
1. /APP_ID/SEND_TAG/a
2. /APP_ID/SEND_TAG/a/+
3. /APP_ID/SEND_TAG/a/+/b...
APP_ID must have the length of AppIdLen, SEND_TAG must be TopicSendOne or TopicSendAll
*/

// DONT change these values!
const (
	TopicSep      = '/'
	TopicWildcard = '+'

	TopicTypeNormal = '1'
	TopicTypeChat   = '2'

	AppIdLen = 10
)

var (
	WildCardHash = utils.MurMurHash([]byte{TopicWildcard})
)

func HashTopic(t []byte) uint32 {
	return utils.MurMurHash(t)
}

func ParseTopic(t []byte, exactly bool) ([]uint32, error) {
	var tids []uint32
	var err error

	if len(t) == 0 {
		err = errors.New("topic cant be empty")
		return nil, err
	}

	var buf []byte
	for i, b := range t {
		if i == 0 { // first byte must be topic sep
			if b != TopicSep {
				err = errors.New("first byte must  be topic sperator")
				return nil, err
			}
			continue
		}
		if i == len(t)-1 { // last byte must not be topic sep
			if b == TopicSep {
				err = errors.New("last byte must not be topic sperator")
				return nil, err
			}
		}
		if b != TopicSep {
			buf = append(buf, b)
			continue
		}

		if len(buf) == 0 {
			err = errors.New("topic invalid,maybe you cant use like '//a' ")
			return nil, err
		}
		tid := utils.MurMurHash(buf)
		tids = append(tids, tid)
		buf = buf[:0]
	}

	if len(buf) != 0 {
		tid := utils.MurMurHash(buf)
		tids = append(tids, tid)
	}

	if len(tids) < 3 {
		return nil, errors.New("topic need contain at least 3 components")
	}

	// first three parts of topic cant be wildcard
	if tids[0] == WildCardHash || tids[1] == WildCardHash || tids[2] == WildCardHash {
		return nil, errors.New("first three parts cant be wildcard")
	}

	// a exactly topic cant contain wildcard char
	if exactly {
		// if the topic is for subscribe,every part of topic cant be wildcard
		for _, tid := range tids[2:] {
			if tid == WildCardHash {
				return nil, errors.New("subscribe topic  cant contain wildcard")
			}
		}
	}

	return tids, nil
}

func AppidAndTopicType(topic []byte) ([]byte, byte, error) {
	i2 := 0
	i3 := 0

	for i, b := range topic {
		if i == 0 {
			// first byte must be topic sep
			if b != TopicSep {
				return nil, 0, errors.New("first byte must  be topic sperator")
			}
			continue
		}
		if i != 0 && b == TopicSep && i2 == 0 {
			i2 = i
			continue
		}
		if b == TopicSep {
			i3 = i
			break
		}
	}

	// last byte cant be topic sep
	if i3 == len(topic)-1 {
		return nil, 0, errors.New("last byte must not be topic sperator")
	}

	// sendtag's length must be 2
	if i3 != i2+2 {
		return nil, 0, errors.New("invalid topic tags")
	}

	// appid's length must be AppIdLen
	appid := topic[1:i2]
	if len(appid) != AppIdLen {
		return nil, 0, errors.New("invalid topic appid")
	}

	typetag := topic[i2+1]
	if typetag != TopicTypeNormal && typetag != TopicTypeChat {
		return nil, 0, errors.New("invalid topic type tag")
	}
	return appid, typetag, nil
}

func GetTopicType(topic []byte) byte {
	return topic[12]
}
