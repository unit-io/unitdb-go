package unitdb

import (
	"errors"
	"sync"
	"time"

	"github.com/unit-io/unitdb-go/utp"
)

const (
	defaultTimeout      = 2 * time.Second
	defaultPoolCapacity = 27
)

type (
	timeID       int64
	batchOptions struct {
		batchDuration       time.Duration
		batchCountThreshold int
		batchByteThreshold  int
		poolCapacity        int
	}
	batch struct {
		count int
		size  int
		r     *PublishResult
		msgs  []*utp.PublishMessage

		timeRefs []timeID
	}
	// batchGroup   map[timeID]*batch
	batchManager struct {
		mu           sync.RWMutex
		batchGroup   map[timeID]*batch
		opts         *batchOptions
		publishQueue chan *batch
		send         chan *batch
		stop         chan struct{}
		stopOnce     sync.Once
		stopWg       sync.WaitGroup
	}
)

func (m *batchManager) newBatch(timeID timeID) *batch {
	b := &batch{
		r:    &PublishResult{result: result{complete: make(chan struct{})}},
		msgs: make([]*utp.PublishMessage, 0),
	}
	m.batchGroup[timeID] = b

	return b
}

func (c *client) newBatchManager(opts *batchOptions) {
	if opts.poolCapacity == 0 {
		opts.poolCapacity = defaultPoolCapacity
	}
	m := &batchManager{
		opts:         opts,
		batchGroup:   make(map[timeID]*batch),
		publishQueue: make(chan *batch, 1),
		send:         make(chan *batch, opts.poolCapacity),
		stop:         make(chan struct{}),
	}

	// timeID of next batch in queue
	m.newBatch(m.TimeID(0))

	// start the publish loop
	go m.publishLoop(defaultBatchDuration)

	// start the commit loop
	m.stopWg.Add(1)
	publishWaitTimeout := c.opts.writeTimeout
	if publishWaitTimeout == 0 {
		publishWaitTimeout = time.Second * 30
	}
	go m.publish(c, publishWaitTimeout)

	// start the dispacther
	m.stopWg.Add(1)
	go m.dispatch(defaultTimeout)

	c.batchManager = m
}

// close tells dispatcher to exit, and wether or not complete queued jobs.
func (m *batchManager) close() {
	m.stopOnce.Do(func() {
		// Close write queue and wait for currently running jobs to finish.
		close(m.stop)
	})
	m.stopWg.Wait()
}

// add adds a publish message to a batch in the batch group.
func (m *batchManager) add(delay int32, p *utp.PublishMessage) *PublishResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	timeID := m.TimeID(delay)
	b, ok := m.batchGroup[timeID]
	if !ok {
		b = m.newBatch(timeID)
	}
	b.msgs = append(b.msgs, p)
	b.count++
	b.size += len(p.Payload)
	if b.count > m.opts.batchCountThreshold || b.size > m.opts.batchByteThreshold {
		m.push(b)
		delete(m.batchGroup, timeID)
	}
	return b.r
}

// push enqueues a batch to publish.
func (m *batchManager) push(b *batch) {
	if len(b.msgs) != 0 {
		m.publishQueue <- b
	}
}

// publishLoop enqueue the publish batches.
func (m *batchManager) publishLoop(interval time.Duration) {
	var publishC <-chan time.Time

	if interval > 0 {
		publishTicker := time.NewTicker(interval)
		defer publishTicker.Stop()
		publishC = publishTicker.C
	}

	for {
		select {
		case <-m.stop:
			timeNow := timeID(TimeNow().UnixNano())
			for timeID, batch := range m.batchGroup {
				if timeID < timeNow {
					m.mu.Lock()
					m.push(batch)
					delete(m.batchGroup, timeID)
					m.mu.Unlock()
				}
			}
			close(m.publishQueue)

			return
		case <-publishC:
			timeNow := timeID(TimeNow().UnixNano())
			for timeID, batch := range m.batchGroup {
				if timeID < timeNow {
					m.mu.Lock()
					m.push(batch)
					delete(m.batchGroup, timeID)
					m.mu.Unlock()
				}
			}
		}
	}
}

// dispatch handles publishing messages for the batches in queue.
func (m *batchManager) dispatch(timeout time.Duration) {
LOOP:
	for {
		select {
		case b, ok := <-m.publishQueue:
			if !ok {
				close(m.send)
				m.stopWg.Done()
				return
			}

			select {
			case m.send <- b:
			default:
				// pool is full, let GC handle the batches
				goto WAIT
			}
		}
	}

WAIT:
	// Wait for a while
	time.Sleep(timeout)
	goto LOOP
}

// publish publishes the messages.
func (m *batchManager) publish(c *client, publishWaitTimeout time.Duration) {
	for {
		select {
		case <-m.stop:
			// run queued messges from the publish queue and
			// process it until queue is empty.
			for {
				select {
				case b, ok := <-m.send:
					if !ok {
						m.stopWg.Done()
						return
					}
					pub := &utp.Publish{Messages: b.msgs}
					mID := c.nextID(b.r)
					pub.MessageID = c.outboundID(mID)

					// persist outbound
					c.storeOutbound(pub)

					select {
					case c.send <- &PacketAndResult{p: pub, r: b.r}:
					case <-time.After(publishWaitTimeout):
						b.r.setError(errors.New("publish timeout error occurred"))
					}
				default:
				}
			}
		case b := <-m.send:
			if b != nil {
				pub := &utp.Publish{Messages: b.msgs}
				mID := c.nextID(b.r)
				pub.MessageID = c.outboundID(mID)

				// persist outbound
				c.storeOutbound(pub)

				select {
				case c.send <- &PacketAndResult{p: pub, r: b.r}:
				case <-time.After(publishWaitTimeout):
					b.r.setError(errors.New("publish timeout error occurred"))
				}
			}
		}
	}
}

func (m *batchManager) TimeID(delay int32) timeID {
	return timeID(TimeNow().Add(m.opts.batchDuration + (time.Duration(delay) * time.Millisecond)).Truncate(m.opts.batchDuration).UnixNano())
}
