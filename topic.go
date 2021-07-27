/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unitdb

import (
	"strings"
	"time"
)

var zeroTime = time.Unix(0, 0)

// Various constant on Topic.
const (
	TopicInvalid = uint8(iota)
)

// topicOption represents a key/value pair option.
type topicOption struct {
	key   string
	value string
}

// topic represents a parsed topic.
type topic struct {
	topic        string // Gets or sets the topic string.
	topicOptions string
	options      []topicOption // Gets or sets the options.
	topicType    uint8
}

// splitFunc various split function to split topic using delimeter.
type splitFunc struct{}

func (splitFunc) options(c rune) bool {
	return c == '?'
}

func (splitFunc) splitOptions(c rune) bool {
	return c == '&'
}

func (splitFunc) splitOpsKeyValue(c rune) bool {
	return c == '='
}

// getOption retrieves a Uint option.
func (t *topic) getOption(name string) (string, bool) {
	for i := 0; i < len(t.options); i++ {
		if t.options[i].key == name {
			return t.options[i].value, true
		}
	}
	return "", false
}

// parseOptions parse the options from the topic.
func (t *topic) parseOptions(text string) (ok bool) {
	//Parse Options
	var fn splitFunc
	ops := strings.FieldsFunc(text, fn.splitOptions)
	if ops != nil || len(ops) >= 1 {
		for _, o := range ops {
			op := strings.FieldsFunc(o, fn.splitOpsKeyValue)
			if op == nil || len(op) < 2 {
				continue
			}
			t.options = append(t.options, topicOption{
				key:   op[0],
				value: op[1],
			})
		}
	}
	return true
}

// parse attempts to parse the topic from the underlying slice.
func (t *topic) parse(text string) (ok bool) {
	// start := time.Now()
	// defer logger.Debug().Str("context", "topic.parseStaticTopic").Dur("duration", time.Since(start)).Msg("")

	var fn splitFunc

	parts := strings.FieldsFunc(text, fn.options)
	l := len(parts)
	if parts == nil || l < 1 {
		t.topicType = TopicInvalid
		return
	}
	if l > 1 {
		t.topicOptions = parts[1]
	}
	t.topic = parts[0]

	ok = t.parseOptions(t.topicOptions)

	if !ok {
		t.topicType = TopicInvalid
		return false
	}

	return true
}
