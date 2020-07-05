	package unitd

	import (
		"sync"
	)

	// MID is 32-bit local message identifier
	type MID uint32

	type messageIds struct {
		sync.RWMutex
		id    MID
		index map[MID]Result // map[MID]Result
	}

	func (mids *messageIds) reset(id MID) {
		mids.Lock()
		defer mids.Unlock()
		mids.id = id
	}

	func (mids *messageIds) freeID(id MID) {
		mids.Lock()
		defer mids.Unlock()
		delete(mids.index, id)
	}

	func (mids *messageIds) nextID(r Result) MID {
		mids.Lock()
		defer mids.Unlock()
		mids.id--
		mids.index[mids.id] = r
		return mids.id
	}

	func (mids *messageIds) getType(id MID) Result {
		mids.RLock()
		defer mids.RUnlock()
		return mids.index[id]
	}
