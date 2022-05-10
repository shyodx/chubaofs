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
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type DentryResponse struct {
	Status uint8
	Msg    *Dentry
}

func NewDentryResponse() *DentryResponse {
	return &DentryResponse{
		Msg: &Dentry{},
	}
}

// Insert a dentry into the dentry tree.
func (mp *metaPartition) fsmCreateDentry(dentry *Dentry, forceUpdate bool) (status uint8) {
	status = proto.OpOk
	tree := mp.GetTree()
	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)
	ikey := InodeKey(InoToBytes(dentry.ParentId))
	ival, err := tree.Get(txn, ikey, INODE)
	if err != nil {
		log.LogInfof("action[fsmCreateDentry] failed to get parentId [%v], dentry name [%v], inode [%v]: %v",
			dentry.ParentId, dentry.Name, dentry.Inode, err)
		return proto.OpIntraGroupNetErr
	}
	log.LogInfof("action[fsmCreateDentry] ParentId [%v] get nil, dentry name [%v], inode [%v]", dentry.ParentId, dentry.Name, dentry.Inode)
	var parIno *Inode
	if !forceUpdate {
		if ival == nil {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get nil, dentry name [%v], inode [%v]", dentry.ParentId, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		parIno = &Inode{}
		if err = parIno.Unmarshal(ival); err != nil {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] unmarshal fail, dentry name [%v], inode [%v]: %v",
				dentry.ParentId, dentry.Name, dentry.Inode)
			return proto.OpIntraGroupNetErr
		}
		if parIno.ShouldDelete() {
			log.LogErrorf("action[fsmCreateDentry] ParentId [%v] get [%v] but should del, dentry name [%v], inode [%v]", dentry.ParentId, parIno, dentry.Name, dentry.Inode)
			status = proto.OpNotExistErr
			return
		}
		if !proto.IsDir(parIno.Type) {
			status = proto.OpArgMismatchErr
			return
		}
	}
	dkey, dval := dentry.ParseKVPair()

	if old, ok := tree.ReplaceOrInsert(txn, dkey, dval, DENTRY, false); !ok {
		d := &Dentry{}
		d.Unmarshal(old)
		//do not allow directories and files to overwrite each
		// other when renaming
		if proto.OsModeType(dentry.Type) != proto.OsModeType(d.Type) {
			status = proto.OpArgMismatchErr
			return
		}

		if dentry.ParentId == d.ParentId && strings.Compare(dentry.Name, d.Name) == 0 && dentry.Inode == d.Inode {
			return
		}

		status = proto.OpExistErr
	} else {
		if !forceUpdate {
			parIno.IncNLink()
			parIno.SetMtime()
		}
	}

	tree.TransactionCommit(txn)

	return
}

// Delete dentry from the dentry tree.
func (mp *metaPartition) fsmDeleteDentry(dentry *Dentry, checkInode bool) (resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	tree := mp.GetTree()

	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	dkey := DentryKey(dentry.MarshalKey())
	if checkInode {
		data, err := tree.Get(txn, dkey, DENTRY)
		if err != nil {
			resp.Status = proto.OpIntraGroupNetErr
			log.LogErrorf("[fsmDeleteDentry] failed to get dentry(%v): %v", dentry.Name, err)
			return
		}
		if data == nil {
			resp.Status = proto.OpNotExistErr
			return
		}

		checkedDentry := &Dentry{}
		if err = checkedDentry.Unmarshal(data); err != nil {
			log.LogErrorf("[fsmDeleteDentry] failed to get dentry(%v): %v", dentry.Name, err)
			resp.Status = proto.OpIntraGroupNetErr
			return
		}
		if checkedDentry.Inode != dentry.Inode {
			resp.Status = proto.OpNotExistErr
			return
		}
	}

	ddata := tree.Delete(txn, dkey, DENTRY)
	if ddata == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	deletedDentry := &Dentry{}
	deletedDentry.Unmarshal(ddata)

	pInode := NewInode(dentry.ParentId, 0)
	ikey := InodeKey(pInode.MarshalKey())
	idata, err := tree.Get(txn, ikey, INODE)
	if err != nil {
		log.LogErrorf("[fsmDeleteDentry] failed to get parent inode(%v): %v", dentry.ParentId, err)
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if idata != nil {
		pInode.Unmarshal(idata)
		if !pInode.ShouldDelete() {
			pInode.DecNLink()
			pInode.SetMtime()
		}

		if idata, err = pInode.Marshal(); err != nil {
			log.LogErrorf("[fsmDeleteDentry] failed to marshal parent inode(%v): %v", pInode.Inode, err)
			resp.Status = proto.OpIntraGroupNetErr
			return

		}
		if err = tree.Put(txn, ikey, idata, INODE); err != nil {
			log.LogErrorf("[fsmDeleteDentry] failed to save updated parent inode(%v): %v", pInode.Inode, err)
			resp.Status = proto.OpIntraGroupNetErr
			return
		}
	}

	tree.TransactionCommit(txn)
	resp.Msg = deletedDentry
	return
}

// batch Delete dentry from the dentry tree.
func (mp *metaPartition) fsmBatchDeleteDentry(db DentryBatch) []*DentryResponse {
	result := make([]*DentryResponse, 0, len(db))
	for _, dentry := range db {
		result = append(result, mp.fsmDeleteDentry(dentry, true))
	}
	return result
}

func (mp *metaPartition) fsmUpdateDentry(dentry *Dentry) (resp *DentryResponse) {
	resp = NewDentryResponse()
	resp.Status = proto.OpOk
	tree := mp.GetTree()

	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	key := DentryKey(dentry.MarshalKey())
	data, err := tree.Get(txn, key, DENTRY)
	if err != nil {
		log.LogErrorf("[fsmUpdateDentry] failed to get dentry(%v): %v", dentry.Name, err)
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if data == nil {
		resp.Status = proto.OpNotExistErr
		return
	}

	d := &Dentry{}
	if err = d.Unmarshal(data); err != nil {
		log.LogErrorf("[fsmUpdateDentry] failed to unmarshal dentry(%v): %v", dentry.Name, err)
		resp.Status = proto.OpIntraGroupNetErr
		return
	}

	dentry.Inode = d.Inode
	if data, err = dentry.Marshal(); err != nil {
		log.LogErrorf("[fsmUpdateDentry] failed to marshal new dentry(%v): %v", dentry.Name, err)
		resp.Status = proto.OpIntraGroupNetErr
		return
	}
	if err = tree.Put(txn, key, data, DENTRY); err != nil {
		log.LogErrorf("[fsmUpdateDentry] failed to save new dentry(%v): %v", dentry.Name, err)
		resp.Status = proto.OpIntraGroupNetErr
		return
	}

	tree.TransactionCommit(txn)
	resp.Msg = dentry
	return
}

func (mp *metaPartition) readDirOnly(req *ReadDirOnlyReq) (resp *ReadDirOnlyResp) {
	resp = &ReadDirOnlyResp{}
	startKey := DentryKey(DentToBytes(req.ParentID, ""))
	endKey := DentryKey(DentToBytes(req.ParentID+1, ""))
	tree := mp.GetTree()

	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	tree.TraverseRange(txn, nil, DENTRY, startKey, endKey, func(data []byte) bool {
		d := &Dentry{}
		if err := d.Unmarshal(data); err != nil {
			log.LogErrorf("[readDirOnly] failed to unmarshal data in parent inode(%v): %v",
				req.ParentID, err)
			// continue
			return true
		}
		if proto.IsDir(d.Type) {
			resp.Children = append(resp.Children, proto.Dentry{
				Inode: d.Inode,
				Type:  d.Type,
				Name:  d.Name,
			})
		}
		return true
	})
	return
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	resp = &ReadDirResp{}
	startKey := DentryKey(DentToBytes(req.ParentID, ""))
	endKey := DentryKey(DentToBytes(req.ParentID+1, ""))
	tree := mp.GetTree()

	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	tree.TraverseRange(txn, nil, DENTRY, startKey, endKey, func(data []byte) bool {
		d := &Dentry{}
		if err := d.Unmarshal(data); err != nil {
			log.LogErrorf("[readDirOnly] failed to unmarshal data in parent inode(%v): %v",
				req.ParentID, err)
			// continue
			return true
		}
		if proto.IsDir(d.Type) {
			resp.Children = append(resp.Children, proto.Dentry{
				Inode: d.Inode,
				Type:  d.Type,
				Name:  d.Name,
			})
		}
		return true
	})
	return
}

// Read dentry from btree by limit count
// if req.Marker == "" and req.Limit == 0, it becomes readDir
// else if req.Marker != "" and req.Limit == 0, return dentries from pid:name to pid+1
// else if req.Marker == "" and req.Limit != 0, return dentries from pid with limit count
// else if req.Marker != "" and req.Limit != 0, return dentries from pid:marker to pid:xxxx with limit count
//
func (mp *metaPartition) readDirLimit(req *ReadDirLimitReq) (resp *ReadDirLimitResp) {
	var startKey DentryKey
	resp = &ReadDirLimitResp{}

	if len(req.Marker) > 0 {
		startKey = DentryKey(DentToBytes(req.ParentID, req.Marker))
	} else {
		startKey = DentryKey(DentToBytes(req.ParentID, ""))
	}
	endKey := DentryKey(DentToBytes(req.ParentID+1, ""))
	tree := mp.GetTree()

	txn, _ := tree.TransactionBegin(TxnDefault)
	defer tree.TransactionEnd(txn, nil)

	tree.TraverseRange(txn, nil, DENTRY, startKey, endKey, func(data []byte) bool {
		d := &Dentry{}
		if err := d.Unmarshal(data); err != nil {
			log.LogErrorf("[readDirOnly] failed to unmarshal data in parent inode(%v): %v",
				req.ParentID, err)
			// continue
			return true
		}
		if proto.IsDir(d.Type) {
			resp.Children = append(resp.Children, proto.Dentry{
				Inode: d.Inode,
				Type:  d.Type,
				Name:  d.Name,
			})
		}
		// Limit == 0 means no limit.
		if req.Limit > 0 && uint64(len(resp.Children)) >= req.Limit {
			return false
		}
		return true
	})
	return
}
