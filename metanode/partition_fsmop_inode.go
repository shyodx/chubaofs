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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type InodeResponse struct {
	Status uint8
	Msg    *Inode
}

func NewInodeResponse() *InodeResponse {
	return &InodeResponse{}
}

// Create and inode and attach it to the inode tree.
func (mp *metaPartition) fsmCreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)

	key := InodeKey(InoToBytes(ino.Inode))
	val, err := ino.Marshal()
	if err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if err = tree.Put(txn, key, val, INODE); err != nil {
		status = proto.OpExistErr
		return
	}

	tree.TransactionCommit(txn)
	tree.TransactionEnd(txn, nil)
	return
}

func (mp *metaPartition) fsmCreateLinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	if err = i.Unmarshal(data); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	i.IncNLink()
	if data, err = i.Marshal(); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if err = tree.Put(txn, key, data, INODE); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	tree.TransactionCommit(txn)
	resp.Msg = i
	return
}

//FIXME: caller may have transaction too
func (mp *metaPartition) getInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	if err = i.Unmarshal(data); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	ctime := Now.GetCurrentTime().Unix()
	/*
	 * FIXME: not protected by lock yet, since nothing is depending on atime.
	 * Shall add inode lock in the future.
	 */
	if ctime > i.AccessTime {
		i.AccessTime = ctime
	}
	if data, err = i.Marshal(); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if err = tree.Put(txn, key, data, INODE); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	tree.TransactionCommit(txn)

	resp.Msg = i
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool) {
	key := InodeKey(ino.MarshalKey())
	data, err := mp.GetTree().GetNoTxn(key, INODE)
	if err != nil {
		panic(err)
	}
	if data == nil {
		ok = false
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	i.Unmarshal(data)
	if i.ShouldDelete() {
		ok = false
		return
	}
	ok = true
	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()
	resp.Status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	inode := NewInode(ino.Inode, ino.Type)
	if err = inode.Unmarshal(data); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if inode.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	resp.Msg = inode

	if inode.IsEmptyDir() {
		tree.Delete(txn, key, INODE)
	}

	inode.DecNLink()

	//Fix#760: when nlink == 0, push into freeList and delay delete inode after 7 days
	if inode.IsTempFile() {
		inode.DoWriteFunc(func() {
			if inode.NLink == 0 {
				inode.AccessTime = time.Now().Unix()
				mp.freeList.Push(inode.Inode)
			}
		})
		data, err = inode.Marshal()
		// save orphan inode until evict
		tree.Put(txn, key, data, INODE)
	}

	tree.TransactionCommit(txn)

	return
}

// fsmUnlinkInode delete the specified inode from inode tree.
func (mp *metaPartition) fsmUnlinkInodeBatch(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmUnlinkInode(ino))
	}
	return
}

func (mp *metaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}
}

func (mp *metaPartition) internalDeleteBatch(val []byte) error {
	if len(val) == 0 {
		return nil
	}
	inodes, err := InodeBatchUnmarshal(val)
	if err != nil {
		return nil
	}

	for _, ino := range inodes {
		log.LogDebugf("internalDelete: received internal delete: partitionID(%v) inode(%v)",
			mp.config.PartitionId, ino.Inode)
		mp.internalDeleteInode(ino)
	}

	return nil
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	ikey := InodeKey(ino.MarshalKey())
	tree.Delete(txn, ikey, INODE)
	mp.freeList.Remove(ino.Inode)
	//mp.extendTree.Delete(&Extend{inode: ino.Inode}) // Also delete extend attribute.
	ekey := ExtendKey(ExtendToBytes())
	tree.Delete(txn, ekey, EXTEND)

	tree.TransactionCommit(txn)
	return
}

func (mp *metaPartition) fsmAppendExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		status = proto.OpNotExistErr
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	if err = i.Unmarshal(data); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if i.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	eks := ino.Extents.CopyExtents()
	delExtents := i.AppendExtents(eks, ino.ModifyTime, mp.volType)

	data, err = i.Marshal()
	tree.Put(txn, key, data, INODE)
	tree.TransactionCommit(txn)

	log.LogInfof("fsmAppendExtents inode(%v) deleteExtents(%v)", i.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmAppendExtentsWithCheck(ino *Inode) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := NewInode(ino.Inode, ino.Type)
	if err = ino2.Unmarshal(data); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	var (
		discardExtentKey []proto.ExtentKey
	)
	eks := ino.Extents.CopyExtents()
	if len(eks) < 1 {
		return
	}
	if len(eks) > 1 {
		discardExtentKey = eks[1:]
	}
	delExtents, status := ino2.AppendExtentWithCheck(eks[0], ino.ModifyTime, discardExtentKey, mp.volType)
	if status == proto.OpOk {
		mp.extDelCh <- delExtents
	}

	// confict need delete eks[0], to clear garbage data
	if status == proto.OpConflictExtentsErr {
		mp.extDelCh <- eks[:1]
	}

	if data, err = ino2.Marshal(); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if err = tree.Put(txn, key, data, INODE); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	tree.TransactionCommit(txn)

	log.LogInfof("fsmAppendExtentWithCheck inode(%v) ek(%v) deleteExtents(%v) discardExtents(%v) status(%v)", ino2.Inode, eks[0], delExtents, discardExtentKey, status)
	return
}

func (mp *metaPartition) fsmAppendObjExtents(ino *Inode) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		status = proto.OpNotExistErr
		return
	}
	inode := NewInode(ino.Inode, ino.Type)
	if err = inode.Unmarshal(data); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if inode.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	eks := ino.ObjExtents.CopyExtents()
	err = inode.AppendObjExtents(eks, ino.ModifyTime)

	// if err is not nil, means obj eks exist overlap.
	if err != nil {
		log.LogErrorf("fsmAppendExtents inode(%v) err(%v)", inode.Inode, err)
		status = proto.OpConflictExtentsErr
	} else {
		data, err = inode.Marshal()
		err = tree.Put(txn, key, data, INODE)
		tree.TransactionCommit(txn)
	}
	return
}

// ino is not point to the member of inodeTree
// it's inode is same with inodeTree,not the extent
// func (mp *metaPartition) fsmDelExtents(ino *Inode) (status uint8) {
// 	status = proto.OpOk
// 	item := mp.inodeTree.CopyGet(ino)
// 	if item == nil {
// 		status = proto.OpNotExistErr
// 		return
// 	}
// 	ino2 := item.(*Inode)
// 	if ino2.ShouldDelete() {
// 		status = proto.OpNotExistErr
// 		return
// 	}
// 	eks := ino.Extents.CopyExtents()
// 	delExtents := ino2.ReplaceExtents(eks, ino.ModifyTime)
// 	log.LogInfof("fsmDelExtents inode(%v) curExtent(%v) delExtents(%v)", ino2.Inode, eks, delExtents)
// 	mp.extDelCh <- delExtents
// 	return
// }

func (mp *metaPartition) fsmExtentsTruncate(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk

	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	if err = i.Unmarshal(data); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}
	if proto.IsDir(i.Type) {
		resp.Status = proto.OpArgMismatchErr
		return
	}

	delExtents := i.ExtentsTruncate(ino.Size, ino.ModifyTime)

	data, err = i.Marshal()
	err = tree.Put(txn, key, data, INODE)
	tree.TransactionCommit(txn)

	// now we should delete the extent
	log.LogInfof("fsmExtentsTruncate inode(%v) exts(%v)", i.Inode, delExtents)
	mp.extDelCh <- delExtents
	return
}

func (mp *metaPartition) fsmEvictInode(ino *Inode) (resp *InodeResponse) {
	resp = NewInodeResponse()

	resp.Status = proto.OpOk

	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	if err = i.Unmarshal(data); err != nil {
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if i.ShouldDelete() {
		resp.Status = proto.OpNotExistErr
		return
	}

	if proto.IsDir(i.Type) {
		if i.IsEmptyDir() {
			i.SetDeleteMark()
		}
	} else if i.IsTempFile() {
		i.SetDeleteMark()
		mp.freeList.Push(i.Inode)
	}

	data, err = i.Marshal()
	err = tree.Put(txn, key, data, INODE)
	tree.TransactionCommit(txn)

	return
}

func (mp *metaPartition) fsmBatchEvictInode(ib InodeBatch) (resp []*InodeResponse) {
	for _, ino := range ib {
		resp = append(resp, mp.fsmEvictInode(ino))
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if proto.IsDir(ino.Type) {
		return
	}
	if ino.ShouldDelete() {
		mp.freeList.Push(ino.Inode)
	} else if ino.IsTempFile() {
		ino.AccessTime = time.Now().Unix()
		mp.freeList.Push(ino.Inode)
	}
}

func (mp *metaPartition) fsmSetAttr(req *SetattrRequest) (err error) {
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(InoToBytes(req.Inode))
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		return
	}
	if data == nil {
		return
	}
	ino := NewInode(req.Inode, req.Mode)
	if err = ino.Unmarshal(data); err != nil {
		return
	}
	if ino.ShouldDelete() {
		return
	}

	ino.SetAttr(req)

	data, err = ino.Marshal()
	err = tree.Put(txn, key, data, INODE)
	tree.TransactionCommit(txn)

	return
}

// fsmExtentsEmpty only use in datalake situation
func (mp *metaPartition) fsmExtentsEmpty(ino *Inode) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		status = proto.OpNotExistErr
		return
	}
	i := NewInode(ino.Inode, ino.Type)
	if err = i.Unmarshal(data); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if i.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	if proto.IsDir(i.Type) {
		status = proto.OpArgMismatchErr
		return
	}
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks len [%v]", mp.config.PartitionId, ino.Inode, len(i.Extents.eks))
	tinyEks := i.CopyTinyExtents()
	log.LogDebugf("action[fsmExtentsEmpty] mp(%d) ino [%v],eks tiny len [%v]", mp.config.PartitionId, ino.Inode, len(tinyEks))

	if len(tinyEks) > 0 {
		mp.extDelCh <- tinyEks
		log.LogDebugf("fsmExtentsEmpty mp(%d) inode(%d) tinyEks(%v)", mp.config.PartitionId, ino.Inode, tinyEks)
	}

	i.EmptyExtents(ino.ModifyTime)
	data, err = i.Marshal()
	err = tree.Put(txn, key, data, INODE)
	tree.TransactionCommit(txn)

	return
}

func (mp *metaPartition) fsmClearInodeCache(ino *Inode) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := InodeKey(ino.MarshalKey())
	data, err := tree.Get(txn, key, INODE)
	if err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		status = proto.OpNotExistErr
		return
	}
	ino2 := NewInode(ino.Inode, ino.Type)
	if err = ino2.Unmarshal(data); err != nil {
		status = proto.OpIntraGroupNetErr
		return
	}
	if ino2.ShouldDelete() {
		status = proto.OpNotExistErr
		return
	}

	delExtents := ino2.EmptyExtents(ino.ModifyTime)
	data, err = ino2.Marshal()
	err = tree.Put(txn, key, data, INODE)
	tree.TransactionCommit(txn)

	log.LogInfof("fsmClearInodeCache inode(%v) delExtents(%v)", ino2.Inode, delExtents)
	if len(delExtents) > 0 {
		mp.extDelCh <- delExtents
	}
	return
}

// attion: unmarshal error will disard extent
func (mp *metaPartition) fsmSendToChan(val []byte) (status uint8) {
	sortExtents := NewSortedExtents()
	err := sortExtents.UnmarshalBinary(val)
	if err != nil {
		panic(fmt.Errorf("[fsmDelExtents] unmarshal sortExtents error, mp(%d), err(%s)", mp.config.PartitionId, err.Error()))
	}

	log.LogInfof("fsmDelExtents mp(%d) delExtents(%v)", mp.config.PartitionId, len(sortExtents.eks))
	mp.extDelCh <- sortExtents.eks
	return
}
