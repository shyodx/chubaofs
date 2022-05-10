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

import "github.com/cubefs/cubefs/util/log"

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) error {
	// FIXME: transaction recursive by UpdateXAttr
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := ExtendKey(ExtendToBytes())

	data, err := tree.Get(txn, key, EXTEND)
	if err != nil {
		log.LogErrorf("[fsmSetXAttr] failed to set xattr: %v", err)
		return err
	}

	var e *Extend
	if data == nil {
		e = NewExtend(extend.inode)
	} else {
		e, err = NewExtendFromBytes(data)
	}
	e.Merge(extend, true)
	data, err = e.Bytes()
	err = tree.Put(txn, key, data, EXTEND)

	tree.TransactionCommit(txn)
	return err
}

func (mp *metaPartition) fsmRemoveXAttr(extend *Extend) error {
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	data, err := tree.Get(txn, ExtendKey(ExtendToBytes()), EXTEND)
	if err != nil {
		log.LogErrorf("[fsmSetXAttr] failed to set xattr: %v", err)
		return err
	}
	if data == nil {
		return err
	}

	e, err := NewExtendFromBytes(data)
	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})

	tree.TransactionCommit(txn)
	return err
}
