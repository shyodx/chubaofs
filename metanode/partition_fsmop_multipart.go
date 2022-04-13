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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) fsmCreateMultipart(multipart *Multipart) (status uint8) {
	_, ok := mp.multipartTree.ReplaceOrInsert(multipart, false)
	if !ok {
		return proto.OpExistErr
	}
	return proto.OpOk
}

func (mp *metaPartition) fsmRemoveMultipart(multipart *Multipart) (status uint8) {
	deletedItem := mp.multipartTree.Delete(multipart)
	if deletedItem == nil {
		return proto.OpNotExistErr
	}
	mp.deleteMultipartFromDB(multipart)
	return proto.OpOk
}

func (mp *metaPartition) fsmAppendMultipart(multipart *Multipart) (status uint8) {
	storedItem := mp.multipartTree.GetForWrite(multipart)
	if storedItem == nil {
		return proto.OpNotExistErr
	}
	defer mp.multipartTree.Put(storedItem)
	storedMultipart, is := storedItem.(*Multipart)
	if !is {
		return proto.OpNotExistErr
	}
	for _, part := range multipart.Parts() {
		actual, stored := storedMultipart.LoadOrStorePart(part)
		if !stored && !actual.Equal(part) {
			return proto.OpExistErr
		}
	}
	return proto.OpOk
}

func (mp *metaPartition) deleteMultipartFromDB(multipart *Multipart) {
	cf, found := mp.metaDB.cfs["multipart"]
	if !found {
		err := errors.New("multipart column family not exist")
		panic(err)
	}

	key := []byte(fmt.Sprintf("%s_%d", multipart.key, multipart.id))
	if _, err := mp.metaDB.db.DelCF(cf, key, false); err != nil {
		log.LogErrorf("Failed to delete multipart %v:%v from DB: %v", multipart.key, multipart.id, err)
		panic(err)
	}
}
