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

import "github.com/cubefs/cubefs/proto"

func (mp *metaPartition) fsmCreateMultipart(multipart *Multipart) (status uint8) {
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := MultipartKey(MultipartToBytes())
	val, err := multipart.Bytes()
	if err != nil {
		return proto.OpIntraGroupNetErr
	}
	_, ok := tree.ReplaceOrInsert(txn, key, val, MULTIPART, false)
	if !ok {
		return proto.OpExistErr
	}

	tree.TransactionCommit(txn)
	return proto.OpOk
}

func (mp *metaPartition) fsmRemoveMultipart(multipart *Multipart) (status uint8) {
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := MultipartKey(MultipartToBytes())
	val := tree.Delete(txn, key, MULTIPART)
	if val == nil {
		return proto.OpNotExistErr
	}

	tree.TransactionCommit(txn)
	return proto.OpOk
}

func (mp *metaPartition) fsmAppendMultipart(multipart *Multipart) (status uint8) {
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := MultipartKey(MultipartToBytes())
	val, err := tree.Get(txn, key, MULTIPART)
	if err != nil {
		return proto.OpIntraGroupNetErr
	}
	if val == nil {
		return proto.OpNotExistErr
	}

	storedMultipart := MultipartFromBytes(val)
	for _, part := range multipart.Parts() {
		actual, stored := storedMultipart.LoadOrStorePart(part)
		if !stored && !actual.Equal(part) {
			return proto.OpExistErr
		}
	}

	if val, err = storedMultipart.Bytes(); err != nil {
		return proto.OpIntraGroupNetErr
	}
	if err = tree.Put(txn, key, val, MULTIPART); err != nil {
		return proto.OpIntraGroupNetErr
	}

	tree.TransactionCommit(txn)
	return proto.OpOk
}
