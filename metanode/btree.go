// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"container/list"
	"sync"

	"github.com/cubefs/cubefs/util/btree"
)

const defaultBTreeDegree = 32

type BtreeItem interface {
	btree.Item
	// ver is protected by Btree lock
	Copy() btree.Item
	GetVersion() uint64
	SetVersion(ver uint64)
	UpdateLRU(head *list.List)
	DeleteLRU(head *list.List)
}

// Btree is the wrapper of Google's btree.
type Btree struct {
	sync.RWMutex
	tree *btree.BTree
	lru  *list.List

	// ver is monotonic and used for clone. Each clone will increase ver.
	// The implementation of BtreeItem also has a ver, if it is different
	// from Btree.ver, modify the implementation should be COWed.
	ver    uint64
	rdonly bool
}

// NewBtree creates a new btree.
func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(defaultBTreeDegree),
		lru:  list.New(),
		ver:  1,
	}
}

// GetForRead returns the a readonly object of the given key in the btree.
func (b *Btree) GetForRead(key btree.Item) (item btree.Item) {
	b.RLock()
	item = b.tree.Get(key)
	if !b.rdonly && item != nil {
		item.(BtreeItem).UpdateLRU(b.lru)
	}
	b.RUnlock()
	return
}

// GetForRead returns the a writable object of the given key in the btree.
func (b *Btree) GetForWrite(key btree.Item) (item btree.Item) {
	b.Lock()
	if b.rdonly {
		panic("Write a read only tree")
	}
	item = b.tree.Get(key)
	if item != nil {
		if item.(BtreeItem).GetVersion() != b.ver {
			// the item will be modified later, we should do a
			// COW for this item, and return the newItem to
			// ensure old item is untouched. The old item could
			// be accessed from the cloned old tree.
			newItem := item.(BtreeItem).Copy()
			newItem.(BtreeItem).SetVersion(b.ver)
			b.tree.ReplaceOrInsert(newItem)
			item = newItem
		}
		item.(BtreeItem).UpdateLRU(b.lru)
	}
	b.Unlock()
	return
}

func (b *Btree) CopyFind(key btree.Item, fn func(i btree.Item)) {
	b.Lock()
	item := b.tree.Get(key)
	if item != nil {
		if b.rdonly {
			panic("Write a read only tree")
		}
		if item.(BtreeItem).GetVersion() != b.ver {
			newItem := item.(BtreeItem).Copy()
			newItem.(BtreeItem).SetVersion(b.ver)
			b.tree.ReplaceOrInsert(newItem)
			item = newItem
		}
	}
	if !b.rdonly && item != nil {
		item.(BtreeItem).UpdateLRU(b.lru)
	}
	fn(item)
	b.Unlock()
}

// Has checks if the key exists in the btree.
func (b *Btree) Has(key btree.Item) (ok bool) {
	b.RLock()
	ok = b.tree.Has(key)
	b.RUnlock()
	return
}

// Delete deletes the object by the given key.
func (b *Btree) Delete(key btree.Item) (item btree.Item) {
	b.Lock()
	if b.rdonly {
		panic("Write a read only tree")
	}
	item = b.tree.Delete(key)
	if item != nil {
		item.(BtreeItem).DeleteLRU(b.lru)
	}
	b.Unlock()
	return
}

// ReplaceOrInsert is the wrapper of google's btree ReplaceOrInsert.
func (b *Btree) ReplaceOrInsert(key btree.Item, replace bool) (item btree.Item, ok bool) {
	b.Lock()
	if b.rdonly {
		panic("Write a read only tree")
	}

	if replace {
		key.(BtreeItem).SetVersion(b.ver)
		item = b.tree.ReplaceOrInsert(key)
		key.(BtreeItem).UpdateLRU(b.lru)
		b.Unlock()
		ok = true
		return
	}

	item = b.tree.Get(key)
	if item == nil {
		key.(BtreeItem).SetVersion(b.ver)
		item = b.tree.ReplaceOrInsert(key)
		key.(BtreeItem).UpdateLRU(b.lru)
		b.Unlock()
		ok = true
		return
	}
	ok = false
	b.Unlock()
	return
}

// Ascend is the wrapper of the google's btree Ascend.
// This function scans the entire btree. When the data is huge, it is not recommended to use this function online.
// Instead, it is recommended to call CloneTree to obtain the snapshot of the current btree, and then do the scan on the snapshot.
func (b *Btree) Ascend(fn func(i btree.Item) bool) {
	b.RLock()
	b.tree.Ascend(fn)
	b.RUnlock()
}

// AscendRange is the wrapper of the google's btree AscendRange.
func (b *Btree) AscendRange(greaterOrEqual, lessThan btree.Item, iterator func(i btree.Item) bool) {
	b.RLock()
	b.tree.AscendRange(greaterOrEqual, lessThan, iterator)
	b.RUnlock()
}

// AscendGreaterOrEqual is the wrapper of the google's btree AscendGreaterOrEqual
func (b *Btree) AscendGreaterOrEqual(pivot btree.Item, iterator func(i btree.Item) bool) {
	b.RLock()
	b.tree.AscendGreaterOrEqual(pivot, iterator)
	b.RUnlock()
}

// GetTree returns the snapshot of a btree.
func (b *Btree) CloneTree() *Btree {
	b.Lock()
	old := b.tree.Clone()
	oldVer := b.ver
	b.ver++
	b.Unlock()
	oldBtree := NewBtree()
	oldBtree.ver = oldVer
	oldBtree.tree = old
	// two trees share the same LRU, but oldBtree does not modify it
	oldBtree.lru = b.lru
	oldBtree.rdonly = true
	return oldBtree
}

// Reset resets the current btree.
func (b *Btree) Reset() {
	b.Lock()
	b.tree.Clear(true)
	b.Unlock()
}

// Len returns the total number of items in the btree.
func (b *Btree) Len() (size int) {
	b.RLock()
	size = b.tree.Len()
	b.RUnlock()
	return
}

// MaxItem returns the largest item in the btree.
func (b *Btree) MaxItem() (item btree.Item) {
	b.RLock()
	item = b.tree.Max()
	b.RUnlock()
	return
}
