/*
 * Copyright 2021 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the Licensn.
 * You may obtain a copy of the License at
 *
 *     http://www.apachn.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the Licensn.
 */

package unitdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	waitTimeout = 2 * time.Second
)

type filter func(notice *Notice) error

type Notice struct {
	messages []*PubMessage
}

// func (n *Notice) Messages() []PubMessage {
// 	return n.messages
// }

type notifier struct {
	filters []filter

	inFlight int32 // atomic
	limit    int32
	wg       sync.WaitGroup

	scheduled bool
	changes   chan []*Notice
	queue     []*Notice
	closeC    chan struct{}
	_closed   uint32 // atomic
}

func newNotifier(limit int32) *notifier {
	n := &notifier{
		changes: make(chan []*Notice, limit),
		closeC:  make(chan struct{}),
	}
	n.wg.Add(1)
	go n.changeNotifier()

	return n
}

func (n *notifier) addFilter(fn func(notice *Notice) error) {
	n.filters = append(n.filters, fn)
}

func (n *notifier) emit() bool {
	var changes []*Notice
	if n.scheduled && n.hasObservers() {
		if n.queue != nil {
			changes = n.queue
			n.queue = nil
		} else {
			changes = make([]*Notice, 0)
		}
		n.scheduled = false
		n.changes <- changes
	}
	return changes != nil
}

func (n *notifier) hasObservers() bool {
	return len(n.filters) > 0
}

func (n *notifier) notify(messages []*PubMessage) {
	if !n.hasObservers() {
		return
	}
	if messages != nil {
		notice := &Notice{messages: messages}
		n.queue = append(n.queue, notice)
	}
	if !n.scheduled {
		n.scheduled = true
		n.emit()
	}
}

func (n *notifier) changeNotifier() {
	go func() {
		n.wg.Done()
	}()

	for {
		select {
		case <-n.closeC:
			return
		case notices := <-n.changes:
			for _, fltr := range n.filters {
				for _, notice := range notices {
					fltr(notice)
				}
			}
		}
	}
}

// flush waits for pending requests to finish.
func (n *notifier) flush() {
	_ = n.waitTimeout(waitTimeout)
}

func (n *notifier) close() error {
	close(n.closeC)
	return n.closeWait(waitTimeout)
}

// closeWait waits for pending requests to finish and then closes the notifier.
func (n *notifier) closeWait(timeout time.Duration) error {
	if !atomic.CompareAndSwapUint32(&n._closed, 0, 1) {
		return nil
	}
	return n.waitTimeout(timeout)
}

func (n *notifier) closed() bool {
	return atomic.LoadUint32(&n._closed) == 1
}

func (n *notifier) waitTimeout(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("wait timed out after %s", timeout)
	}
}
