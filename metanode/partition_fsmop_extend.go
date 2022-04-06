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
	"fmt"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type ExtendOpResult struct {
	Status uint8
	Extend *Extend
}

func (mp *metaPartition) fsmSetXAttr(extend *Extend) (err error) {
	treeItem := mp.extendTree.GetForWrite(extend)
	var e *Extend
	if treeItem == nil {
		e = NewExtend(extend.inode)
		mp.extendTree.ReplaceOrInsert(e, true)
	} else {
		e = treeItem.(*Extend)
	}
	e.Merge(extend, true)
	return
}

func (mp *metaPartition) fsmRemoveXAttr(extend *Extend) (err error) {
	treeItem := mp.extendTree.GetForWrite(extend)
	if treeItem == nil {
		return
	}
	e := treeItem.(*Extend)
	extend.Range(func(key, value []byte) bool {
		e.Remove(key)
		return true
	})
	return
}

func (mp *metaPartition) deleteExtendsFromDB(e *Extend) {
	cf, found := mp.metaDB.cfs["extend"]
	if !found {
		err := errors.New("extend column family not exist")
		panic(err)
	}

	key := []byte(fmt.Sprintf("%d", e.inode))
	if _, err := mp.metaDB.db.DelCF(cf, key, false); err != nil {
		log.LogErrorf("Failed to delete extend %v from DB: %v", e.inode, err)
		panic(err)
	}
}
