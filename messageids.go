package unitdb

import (
	"sync"
)

// MID is 32-bit local message identifier
type MID uint32

type messageIds struct {
	sync.RWMutex
	id    MID
	resumedIds map[MID]struct{}
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

func (mids *messageIds) resumeID(id MID) {
	mids.Lock()
	defer mids.Unlock()
	mids.resumedIds[id] = struct{}{}
}

func (mids *messageIds) nextID(r Result) MID {
	mids.Lock()
	defer mids.Unlock()
	mids.id--
	if _,ok:=mids.resumedIds[mids.id];ok{
		mids.nextID(r)
	}
	mids.index[mids.id] = r
	return mids.id
}

func (mids *messageIds) getType(id MID) Result {
	mids.RLock()
	defer mids.RUnlock()
	return mids.index[id]
}
