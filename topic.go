/*
 * Copyright 2021 Saffat Technologies, Ltd.
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
	"errors"
	"fmt"
	"strings"
)

// Various constant on Topic.
const (
	TopicInvalid = uint8(iota)
	TopicStatic
	TopicWildcard
	TopicWildcardSymbol      = "*"
	TopicMultiWildcardSymbol = "..."
	TopicKeySeparator        = '/'
	TopicSeparator           = '.' // The separator character.
	TopicMaxLength           = 65535
	TopicMaxDepth            = 100 // Maximum depth for topic using a separator
)

// topicOption represents a key/value pair option.
type topicOption struct {
	key   string
	value string
}

// topic represents a parsed topic.
type (
	topic struct {
		topic        string // Gets or sets the topic string.
		parts        []string
		topicOptions string
		options      []topicOption // Gets or sets the options.
		topicType    uint8
	}
	TopicFilter struct {
		subscriptionTopic *topic
		updates           chan []*PubMessage
	}
)

func (t *TopicFilter) Updates() <-chan []*PubMessage {
	return t.updates
}

// splitFunc various split function to split topic using delimeter.
type splitFunc struct{}

func (splitFunc) splitTopic(c rune) bool {
	return c == TopicSeparator
}

func (splitFunc) splitKey(c rune) bool {
	return c == TopicKeySeparator
}

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

	parts := strings.FieldsFunc(text, fn.splitKey)
	if parts == nil || len(parts) < 1 {
		t.topicType = TopicInvalid
		return
	}

	t.topic = parts[0]
	if len(parts) > 1 {
		t.topic = parts[1]
	}

	parts = strings.FieldsFunc(t.topic, fn.options)
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

	var multiWildcard bool
	if strings.HasSuffix(t.topic, TopicMultiWildcardSymbol) {
		t.topic = strings.TrimRight(t.topic, TopicMultiWildcardSymbol)
		multiWildcard = true
	}

	t.parts = strings.FieldsFunc(t.topic, fn.splitTopic)

	if multiWildcard {
		t.parts = append(t.parts, TopicMultiWildcardSymbol)
	}

	return true
}

func (t *topic) validate(fn ...func(t *topic) error) error {
	for _, f := range fn {
		if err := f(t); err != nil {
			return err
		}
	}

	return nil
}

func validateWildcards(t *topic) error {
	if strings.Contains(t.topic, TopicMultiWildcardSymbol) || strings.Contains(t.topic, TopicWildcardSymbol) {
		return errors.New("publish: cannot publish to a topic that contains topic wildcard (* or ...)")
	}

	return nil
}

func validateMinLength(t *topic) error {
	if len(t.topic) == 0 {
		return errors.New("topic must contain at least one character")
	}

	return nil
}

func validateMaxLenth(t *topic) error {
	if len(t.topic) > TopicMaxLength {
		return fmt.Errorf("the length of topic %d is longer than the max topic length allowed %d", len(t.topic), TopicMaxLength)
	}

	return nil
}

func validateMaxDepth(t *topic) error {
	if len(t.parts) > TopicMaxDepth {
		return fmt.Errorf("the depath of topic %d is longer than the max topic length allowed %d", len(t.parts), TopicMaxDepth)
	}

	return nil
}

func validateMultiWildcard(t *topic) error {
	if strings.Contains(t.topic, TopicMultiWildcardSymbol) &&
		!(strings.HasSuffix(t.topic, TopicMultiWildcardSymbol)) {
		return errors.New("the topic multiwild ... can only be present at the end of a topic")
	}

	return nil
}

func validateTopicParts(t *topic) error {
	for _, part := range t.parts {
		if strings.Contains(part, TopicWildcardSymbol) && len(part) > 1 {
			return errors.New("topic part contains a wildcard but is more than one character long")
		}
	}

	return nil
}

func (t *TopicFilter) filter(notice *Notice) error {
	messages := make([]*PubMessage, 0)

	for _, pubMsg := range notice.messages {
		pubTopic := new(topic)

		// parse the topic.
		if ok := pubTopic.parse(pubMsg.Topic); !ok {
			return fmt.Errorf("filter: unable to parse topic")
		}

		if err := pubTopic.validate(validateMinLength,
			validateMaxLenth,
			validateMaxDepth,
			validateMultiWildcard,
			validateTopicParts); err != nil {
			return err
		}

		if t.subscriptionTopic.matches(pubTopic) {
			messages = append(messages, notice.messages...)
		}
	}

	if len(messages) > 0 {
		t.updates <- messages
	}

	return nil
}

func (t *topic) matches(pubTopic *topic) bool {
	// If the topic is just multi wildcard then return matches to true.
	if t.topic == TopicMultiWildcardSymbol ||
		pubTopic.topic == TopicWildcardSymbol {
		return true
	}

	// If the topics are an exact match then matches to true.
	if t.topic == pubTopic.topic {
		return true
	}

	// no match yet so we need to check each part
	for i := 0; i < len(t.parts); i++ {
		lhsPart := t.parts[i]
		// If we've reached a multi wildcard in the lhs topic,
		// we have a match.
		// (this is the rule finance matches finance or finance...)
		if lhsPart == TopicMultiWildcardSymbol {
			return true
		}
		isLhsWildcard := lhsPart == TopicWildcardSymbol
		// If we've reached a wildcard match but the matchee does
		// not have anything at this part level then it's not a match.
		// (this is the rule 'finance does not match finance.*'
		if isLhsWildcard && len(pubTopic.parts) <= i {
			return false
		}
		// if lhs is not a wildcard we need to check whether the
		// two parts match each other.
		if !isLhsWildcard {
			rhsPart := pubTopic.parts[i]
			// If the lhs part is not wildcard then we need an exact match
			if lhsPart != rhsPart {
				return false
			}
		}
		// If we're at the last part of the lhs topic but there are
		// more patrs in the in the publicationTopic then the publicationTopic
		// is too specific to be a match.
		if i+1 == len(t.parts) &&
			len(pubTopic.parts) > len(t.parts) {
			return false
		}
		if i+1 == len(pubTopic.parts) &&
			len(pubTopic.parts) < len(t.parts) {
			return false
		}
		// If we're here the current part matches so check the next
	}
	// If we exit out of the loop without a return then we have a full match which would
	// have been caught by the original exact match check at the top anyway.
	return true
}
