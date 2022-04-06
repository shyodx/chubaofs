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
	"bufio"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tecbot/gorocksdb"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	mmap "github.com/edsrzf/mmap-go"
)

const (
	snapshotDir     = "snapshot"
	snapshotDirTmp  = ".snapshot"
	snapshotBackup  = ".snapshot_backup"
	inodeFile       = "inode"
	dentryFile      = "dentry"
	extendFile      = "extend"
	multipartFile   = "multipart"
	applyIDFile     = "apply"
	SnapshotSign    = ".sign"
	metadataFile    = "meta"
	metadataFileTmp = ".meta"

	metaDBDir              = "metadb"
	CheckpointDir          = "checkpoint"
	OldCheckpointDir       = "checkpoint.old"
	NewCheckpointDir       = "checkpoint.new"
	CheckpointDirExpPrefix = "expired_checkpoint_"
)

type InodeStats struct {
	ver    uint32 // HARD CODED, keep ver the first element
	rsvd   uint32
	total  uint64
	cursor uint64
}

type InodeStatsVerion struct {
	Ver  uint32
	Size uint32
}

var ISVer1 = &InodeStatsVerion{1, 24}

func (is *InodeStats) String() string {
	return fmt.Sprintf("Ver(%v) Total(%v) Cursor(%v)", is.ver, is.total, is.cursor)
}

func (is *InodeStats) Marshal() []byte {
	var data []byte
	switch is.ver {
	case ISVer1.Ver:
		data = make([]byte, ISVer1.Size)
		binary.BigEndian.PutUint32(data[0:4], is.ver)
		binary.BigEndian.PutUint64(data[8:16], is.total)
		binary.BigEndian.PutUint64(data[16:24], is.cursor)
	}
	return data
}

func (is *InodeStats) Unmarshal(data []byte) {
	is.ver = binary.BigEndian.Uint32(data[0:4])
	switch is.ver {
	case ISVer1.Ver:
		if len(data) != int(ISVer1.Size) {
			break
		}
		is.total = binary.BigEndian.Uint64(data[8:16])
		is.cursor = binary.BigEndian.Uint64(data[16:24])
		return
	}
	msg := fmt.Sprintf("Unrecognize InodeStats: ver[%v] len[%v]", is.ver, len(data))
	panic(msg)
}

func (mp *metaPartition) loadMetadata() (err error) {
	metaFile := path.Join(mp.config.RootDir, metadataFile)
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadMetadata]: OpenFile %s", err.Error())
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		err = errors.NewErrorf("[loadMetadata]: ReadFile %s, data: %s", err.Error(),
			string(data))
		return
	}
	mConf := &MetaPartitionConfig{}
	if err = json.Unmarshal(data, mConf); err != nil {
		err = errors.NewErrorf("[loadMetadata]: Unmarshal MetaPartitionConfig %s",
			err.Error())
		return
	}

	if mConf.checkMeta() != nil {
		return
	}
	mp.config.PartitionId = mConf.PartitionId
	mp.config.VolName = mConf.VolName
	mp.config.Start = mConf.Start
	mp.config.End = mConf.End
	mp.config.Peers = mConf.Peers
	mp.config.Cursor = mp.config.Start

	log.LogInfof("loadMetadata: load complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	log.LogCriticalf("DEBUG: loadMetadata: load complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	return
}

func (mp *metaPartition) loadInode(rootDir string) (err error) {
	var numInodes uint64

	filename := path.Join(rootDir, snapshotDir, inodeFile)
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			dir := path.Join(rootDir, metaDBDir)
			log.LogCriticalf("DEBUG: [loadInode] Part(%v) load from [%s]", mp.config.RootDir, dir)
			if err = mp.loadInodeFromDB(dir); err != nil {
				return
			}
		}
		return
	}
	log.LogCriticalf("DEBUG: [loadInode] Part(%v) load from [%s]", mp.config.RootDir, filename)
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = errors.NewErrorf("[loadInode] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	inoBuf := make([]byte, 4)
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadInode] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(inoBuf)

		// next read body
		if uint32(cap(inoBuf)) >= length {
			inoBuf = inoBuf[:length]
		} else {
			inoBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			err = errors.NewErrorf("[loadInode] ReadBody: %s", err.Error())
			return
		}
		ino := NewInode(0, 0)
		if err = ino.Unmarshal(inoBuf); err != nil {
			err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
			return
		}
		mp.fsmCreateInode(ino)
		mp.checkAndInsertFreeList(ino)
		if mp.config.Cursor < ino.Inode {
			mp.config.Cursor = ino.Inode
		}
		ino.MarkReady()
		numInodes += 1
	}

	log.LogInfof("loadinode: load complete: partitonid(%v) volume(%v) numinodes(%v)",
		mp.config.PartitionId, mp.config.VolName, numInodes)
	log.LogCriticalf("DEBUG: loadinode: load complete: partitonid(%v) volume(%v) numinodes(%v)",
		mp.config.PartitionId, mp.config.VolName, numInodes)

	return
}

// Load dentry from the dentry snapshot.
func (mp *metaPartition) loadDentry(rootDir string) (err error) {
	var numDentries uint64

	filename := path.Join(rootDir, snapshotDir, dentryFile)
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			dir := path.Join(rootDir, metaDBDir)
			log.LogCriticalf("DEBUG: [loadDentry] Part(%v) from [%v]", mp.config.RootDir, dir)
			if err = mp.loadDentryFromDB(dir); err != nil {
				return
			}
		}
		return
	}
	log.LogCriticalf("DEBUG: [loadDentry] Part(%v) from [%v]", mp.config.RootDir, filename)
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadDentry] OpenFile: %s", err.Error())
		return
	}

	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	dentryBuf := make([]byte, 4)
	for {
		dentryBuf = dentryBuf[:4]
		// First Read 4byte header length
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDentry] ReadHeader: %s", err.Error())
			return
		}

		length := binary.BigEndian.Uint32(dentryBuf)

		// next read body
		if uint32(cap(dentryBuf)) >= length {
			dentryBuf = dentryBuf[:length]
		} else {
			dentryBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDentry]: ReadBody: %s", err.Error())
			return
		}
		dentry := &Dentry{}
		if err = dentry.Unmarshal(dentryBuf); err != nil {
			err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		if status := mp.fsmCreateDentry(dentry, true); status != proto.OpOk {
			err = errors.NewErrorf("[loadDentry] createDentry dentry: %v, resp code: %d", dentry, status)
			return
		}
		numDentries += 1
	}

	log.LogInfof("loadDentry: load complete: partitonID(%v) volume(%v) numDentries(%v)",
		mp.config.PartitionId, mp.config.VolName, numDentries)
	log.LogCriticalf("DEBUG: loadDentry: load complete: partitonID(%v) volume(%v) numDentries(%v)",
		mp.config.PartitionId, mp.config.VolName, numDentries)
	return
}

func (mp *metaPartition) loadExtend(rootDir string) error {
	var err error
	filename := path.Join(rootDir, snapshotDir, extendFile)
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			dir := path.Join(rootDir, metaDBDir)
			if err = mp.loadExtendFromDB(dir); err != nil {
				return err
			}
		}
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = fp.Close()
	}()
	var mem mmap.MMap
	if mem, err = mmap.Map(fp, mmap.RDONLY, 0); err != nil {
		return err
	}
	defer func() {
		_ = mem.Unmap()
	}()
	var offset, n int
	// read number of extends
	var numExtends uint64
	numExtends, n = binary.Uvarint(mem)
	offset += n
	for i := uint64(0); i < numExtends; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var extend *Extend
		if extend, err = NewExtendFromBytes(mem[offset : offset+int(numBytes)]); err != nil {
			return err
		}
		log.LogDebugf("loadExtend: new extend from bytes: partitionID（%v) volume(%v) inode(%v)",
			mp.config.PartitionId, mp.config.VolName, extend.inode)
		_ = mp.fsmSetXAttr(extend)
		offset += int(numBytes)
	}
	log.LogInfof("loadExtend: load complete: partitionID(%v) volume(%v) numExtends(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, numExtends, filename)
	return nil
}

func (mp *metaPartition) loadMultipart(rootDir string) error {
	var err error
	filename := path.Join(rootDir, snapshotDir, multipartFile)
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			dir := path.Join(rootDir, metaDBDir)
			if err = mp.loadMultipartFromDB(dir); err != nil {
				return err
			}
		}
		return nil
	}
	fp, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		_ = fp.Close()
	}()
	var mem mmap.MMap
	if mem, err = mmap.Map(fp, mmap.RDONLY, 0); err != nil {
		return err
	}
	defer func() {
		_ = mem.Unmap()
	}()
	var offset, n int
	// read number of extends
	var numMultiparts uint64
	numMultiparts, n = binary.Uvarint(mem)
	offset += n
	for i := uint64(0); i < numMultiparts; i++ {
		// read length
		var numBytes uint64
		numBytes, n = binary.Uvarint(mem[offset:])
		offset += n
		var multipart *Multipart
		multipart = MultipartFromBytes(mem[offset : offset+int(numBytes)])
		log.LogDebugf("loadMultipart: create multipart from bytes: partitionID（%v) multipartID(%v)", mp.config.PartitionId, multipart.id)
		mp.fsmCreateMultipart(multipart)
		offset += int(numBytes)
	}
	log.LogInfof("loadMultipart: load complete: partitionID(%v) numMultiparts(%v) filename(%v)",
		mp.config.PartitionId, numMultiparts, filename)
	return nil
}

func (mp *metaPartition) loadApplyID(rootDir string) (err error) {
	filename := path.Join(rootDir, snapshotDir, applyIDFile)
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			filename = path.Join(rootDir, metaDBDir, applyIDFile)
			if _, err = os.Stat(filename); err != nil {
				err = nil
				return
			}
		}
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadApplyID] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyID]: ApplyID is empty")
		return
	}
	var cursor uint64
	if strings.Contains(string(data), "|") {
		_, err = fmt.Sscanf(string(data), "%d|%d", &mp.applyID, &cursor)
	} else {
		_, err = fmt.Sscanf(string(data), "%d", &mp.applyID)
	}
	if err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}

	if cursor > atomic.LoadUint64(&mp.config.Cursor) {
		atomic.StoreUint64(&mp.config.Cursor, cursor)
	}
	log.LogInfof("loadApplyID: load complete: partitionID(%v) volume(%v) applyID(%v) filename(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.applyID, filename)
	return
}

func (mp *metaPartition) persistMetadata() (err error) {
	if err = mp.config.checkMeta(); err != nil {
		err = errors.NewErrorf("[persistMetadata]->%s", err.Error())
		return
	}

	// TODO Unhandled errors
	os.MkdirAll(mp.config.RootDir, 0755)
	filename := path.Join(mp.config.RootDir, metadataFileTmp)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		// TODO Unhandled errors
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()

	data, err := json.Marshal(mp.config)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	if err = os.Rename(filename, path.Join(mp.config.RootDir, metadataFile)); err != nil {
		return
	}
	log.LogInfof("persistMetata: persist complete: partitionID(%v) volume(%v) range(%v,%v) cursor(%v)",
		mp.config.PartitionId, mp.config.VolName, mp.config.Start, mp.config.End, mp.config.Cursor)
	return
}

func (mp *metaPartition) storeApplyID(rootDir string, sm *storeMsg) (err error) {
	filename := path.Join(rootDir, applyIDFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		fp.Close()
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d|%d", sm.applyIndex, atomic.LoadUint64(&mp.config.Cursor))); err != nil {
		return
	}
	log.LogInfof("storeApplyID: store complete: partitionID(%v) volume(%v) applyID(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.applyIndex)
	return
}

func (mp *metaPartition) storeInode(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, inodeFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.inodeTree.Ascend(func(i btree.Item) bool {
		ino := i.(*Inode)
		if data, err = ino.Marshal(); err != nil {
			return false
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}
		// set body
		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})
	crc = sign.Sum32()
	log.LogInfof("storeInode: store complete: partitoinID(%v) volume(%v) numInodes(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), crc)
	return
}

func (mp *metaPartition) storeDentry(rootDir string,
	sm *storeMsg) (crc uint32, err error) {
	filename := path.Join(rootDir, dentryFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		err = fp.Sync()
		// TODO Unhandled errors
		fp.Close()
	}()
	var data []byte
	lenBuf := make([]byte, 4)
	sign := crc32.NewIEEE()
	sm.dentryTree.Ascend(func(i btree.Item) bool {
		dentry := i.(*Dentry)
		data, err = dentry.Marshal()
		if err != nil {
			return false
		}
		// set length
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err = fp.Write(lenBuf); err != nil {
			return false
		}
		if _, err = sign.Write(lenBuf); err != nil {
			return false
		}
		if _, err = fp.Write(data); err != nil {
			return false
		}
		if _, err = sign.Write(data); err != nil {
			return false
		}
		return true
	})
	crc = sign.Sum32()
	log.LogInfof("storeDentry: store complete: partitoinID(%v) volume(%v) numDentries(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.dentryTree.Len(), crc)
	return
}

func (mp *metaPartition) storeExtend(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var extendTree = sm.extendTree
	var fp = path.Join(rootDir, extendFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	var writer = bufio.NewWriterSize(f, 4*1024*1024)
	var crc32 = crc32.NewIEEE()
	var varintTmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(extendTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	extendTree.Ascend(func(i btree.Item) bool {
		e := i.(*Extend)
		var raw []byte
		if raw, err = e.Bytes(); err != nil {
			return false
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false
		}
		if _, err = crc32.Write(raw); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return
	}

	if err = writer.Flush(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	crc = crc32.Sum32()
	log.LogInfof("storeExtend: store complete: partitoinID(%v) volume(%v) numExtends(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, extendTree.Len(), crc)
	return
}

func (mp *metaPartition) storeMultipart(rootDir string, sm *storeMsg) (crc uint32, err error) {
	var multipartTree = sm.multipartTree
	var fp = path.Join(rootDir, multipartFile)
	var f *os.File
	f, err = os.OpenFile(fp, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	var writer = bufio.NewWriterSize(f, 4*1024*1024)
	var crc32 = crc32.NewIEEE()
	var varintTmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// write number of extends
	n = binary.PutUvarint(varintTmp, uint64(multipartTree.Len()))
	if _, err = writer.Write(varintTmp[:n]); err != nil {
		return
	}
	if _, err = crc32.Write(varintTmp[:n]); err != nil {
		return
	}
	multipartTree.Ascend(func(i btree.Item) bool {
		m := i.(*Multipart)
		var raw []byte
		if raw, err = m.Bytes(); err != nil {
			return false
		}
		// write length
		n = binary.PutUvarint(varintTmp, uint64(len(raw)))
		if _, err = writer.Write(varintTmp[:n]); err != nil {
			return false
		}
		if _, err = crc32.Write(varintTmp[:n]); err != nil {
			return false
		}
		// write raw
		if _, err = writer.Write(raw); err != nil {
			return false
		}
		if _, err = crc32.Write(raw); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return
	}

	if err = writer.Flush(); err != nil {
		return
	}
	if err = f.Sync(); err != nil {
		return
	}
	crc = crc32.Sum32()
	log.LogInfof("storeMultipart: store complete: partitoinID(%v) volume(%v) numMultiparts(%v) crc(%v)",
		mp.config.PartitionId, mp.config.VolName, multipartTree.Len(), crc)
	return
}

func (mp *metaPartition) storeInodeToDB(sm *storeMsg) (err error) {
	var (
		data    []byte
		saveAll bool
		cnt     uint64
	)

	orphanCF, found := mp.metaDB.cfs["orphaninode"]
	if !found {
		err = errors.New("orphan inode column family not found")
		return
	}
	inodeCF, found := mp.metaDB.cfs["inode"]
	if !found {
		err = errors.New("inode column family not found")
		return
	}

	// save all orphan inodes
	orphanHandler := func(item *list.Element) error {
		// FIXME: should use PutBatchCF
		// For example, add two funcs: PrepareBatchCF and WriteBatch
		// call PrepareBathCF to put ino into batch, and every N items,
		// call WriteBatch to write the batch
		ino := item.Value.(uint64)
		key := []byte(fmt.Sprintf("%v", ino))
		log.LogCriticalf("DEBUG: Part(%v) save ino %v to orphaninode cf", mp.config.PartitionId, ino)
		if e := mp.metaDB.db.PutCF(orphanCF, key, nil, false); e != nil {
			return e
		}
		return nil
	}
	if err = mp.freeList.ForEach(orphanHandler); err != nil {
		return
	}

	// save all dirty inodes
	// if it is the first time to save inodes to DB, save them all
	if mp.metaDB.TestStatus(MetaDBNew) {
		saveAll = true
	}
	inodeHandler := func(item btree.Item) bool {
		// inode is COW, so no need to lock it
		inode := item.(*Inode)
		if saveAll || inode.IsDirty() {
			log.LogCriticalf("DEBUG: Part(%v) save ino %v to inode cf", mp.config.PartitionId, inode.Inode)
			key := []byte(fmt.Sprintf("%v", inode.Inode))
			if data, err = inode.Marshal(); err != nil {
				return false
			}
			if err = mp.metaDB.db.PutCF(inodeCF, key, data, false); err != nil {
				return false
			}
			cnt++
		}
		return true
	}
	sm.inodeTree.Ascend(inodeHandler)

	// save InodeStats
	// FIXME: we only have a COW on the tree, but Cursor is not COW values
	// FIXME: a larger cursor is safe?
	is := &InodeStats{
		ver:    ISVer1.Ver,
		total:  uint64(sm.inodeTree.Len()),
		cursor: mp.config.Cursor,
	}
	if err = mp.metaDB.db.PutCF(inodeCF, []byte("0"), is.Marshal(), false); err != nil {
		return err
	}

	log.LogInfof("storeInodeToDB: store complete: partitoinID(%v) volume(%v) numInodes(%v) dirytInodes(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), cnt)
	log.LogCriticalf("DEBUG: storeInodeToDB: store complete: partitoinID(%v) volume(%v) numInodes(%v) dirytInodes(%v) saveAll(%v) st(%x)",
		mp.config.PartitionId, mp.config.VolName, sm.inodeTree.Len(), cnt, saveAll, mp.metaDB.status)

	return
}

func (mp *metaPartition) loadInodeFromDB(dir string) (err error) {
	var (
		orphanCF *gorocksdb.ColumnFamilyHandle
		inodeCF  *gorocksdb.ColumnFamilyHandle
		data     interface{}
		is       InodeStats
		found    bool
		nr       uint64
	)

	if _, err = os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return
	}

	if orphanCF, found = mp.metaDB.cfs["orphaninode"]; !found {
		err = fmt.Errorf("orphaninode column family not exist")
		log.LogError(err)
		log.LogCriticalf("DEBUG: [loadInodeFromDB] Part(%v): %v", mp.config.RootDir, err)
		return
	}
	if inodeCF, found = mp.metaDB.cfs["inode"]; !found {
		err = fmt.Errorf("inode column family not exist")
		log.LogError(err)
		log.LogCriticalf("DEBUG: [loadInodeFromDB] Part(%v): %v", mp.config.RootDir, err)
		return
	}
	db := mp.metaDB.db

	// load all orphan inodes
	orphInoStr := make([]string, 0)
	loadOphanFunc := func(k, v *gorocksdb.Slice) error {
		inoStr := string(v.Data())
		log.LogCriticalf("DEBUG: Part(%v) load ino(%v) from orphaninode cf", mp.config.PartitionId, inoStr)
		orphInoStr = append(orphInoStr, inoStr)
		return nil
	}
	if nr, err = db.LoadNCF(orphanCF, 0, loadOphanFunc, nil); err != nil {
		log.LogCriticalf("DEBUG: [loadInodeFromDB] load orphan Part(%v): %v", mp.config.RootDir, err)
		panic(err)
	}

	for _, inoStr := range orphInoStr {
		data, err := db.GetCF(inodeCF, []byte(inoStr))
		if err != nil {
			log.LogCriticalf("DEBUG: Failed Part(%v) load orphan ino(%v) from inode cf: %v", mp.config.PartitionId, inoStr, err)
			err = fmt.Errorf("Failed load Part(%v) orphan ino(%v) from orphaninode cf: %v",
				mp.config.PartitionId, inoStr, err)
			return err
		}
		inode := NewInode(0, 0)
		if err = inode.Unmarshal(data.([]byte)); err != nil {
			log.LogCriticalf("DEBUG: Failed Part(%v) unmarshal ino(%v): %v", mp.config.PartitionId, inoStr, err)
			err = fmt.Errorf("Failed unmarshal Part(%v) ino(%v): %v",
				mp.config.PartitionId, inoStr, err)
			return err
		}
		mp.fsmCreateInode(inode)
		mp.checkAndInsertFreeList(inode)
		inode.MarkReady()
	}

	log.LogInfof("loadInodeFromDB: load orphan complete: partitonid(%v) volume(%v) numinodes(%v)",
		mp.config.PartitionId, mp.config.VolName, nr)
	log.LogCriticalf("DEBUG: loadInodeFromDB: load orphan complete: partitonid(%v) volume(%v) numinodes(%v)",
		mp.config.PartitionId, mp.config.VolName, nr)

	// load all inodes
	// FIXME: load the first inode only?
	// FIXME: can we put inodes as LRU order in rocksdb, and load them as
	// the order back to memory? If it could, we could load the first N inodes
	loadInodeFunc := func(k, v *gorocksdb.Slice) error {
		inode := NewInode(0, 0)
		if e := inode.Unmarshal(v.Data()); e != nil {
			return errors.NewErrorf("[loadInodeFromDB] Unmarshal: %v", e)
		}
		log.LogCriticalf("DEBUG: Part(%v) load ino(%v) from inode cf", mp.config.PartitionId, inode.Inode)
		if inode.NLink == 0 {
			// skip orphan inodes
			return nil
		}
		mp.fsmCreateInode(inode)
		inode.MarkReady()
		return nil
	}
	skip := [][]byte{[]byte("0")}
	if nr, err = db.LoadNCF(inodeCF, 0, loadInodeFunc, skip); err != nil {
		log.LogCriticalf("DEBUG: [loadInodeFromDB] load inode Part(%v): %v", mp.config.RootDir, err)
		panic(err)
	}

	// load InodeStat
 	// FIXME: get max ino correctly
 	// FIXME: use 0 as a specific value for inodes?
 	// FIXME: key should be string or raw array?
	key := []byte(fmt.Sprintf("%v", 0))
	if data, err = db.GetCF(inodeCF, key); err != nil {
		log.LogErrorf("[loadInodeFromDB] load statistics info: %v", err)
		log.LogCriticalf("DEBUG: [loadInodeFromDB] load statistics info Part(%v): %v", mp.config.RootDir, err)
		return
	}
	is.Unmarshal(data.([]byte))

	mp.config.Cursor = is.cursor
	log.LogInfof("loadInodeFromDB: load complete: partitonid(%v) volume(%v) numinodes(%v) IS(%v)",
		mp.config.PartitionId, mp.config.VolName, nr, is.String())
	log.LogCriticalf("DEBUG: loadInodeFromDB: load complete: partitonid(%v) volume(%v) numinodes(%v) IS(%v)",
		mp.config.PartitionId, mp.config.VolName, nr, is.String())

	return
}

func (mp *metaPartition) storeDentryToDB(sm *storeMsg) (err error) {
	var (
		data    []byte
		saveAll bool
		cnt     uint64
	)

	cf, found := mp.metaDB.cfs["dentry"]
	if !found {
		err = errors.New("dentry column family not found")
		return
	}

	// save all dirty dentries
	// if it is the first time to save dentries to DB, save them all
	if mp.metaDB.TestStatus(MetaDBNew) {
		saveAll = true
	}
	dentryHandler := func(item btree.Item) bool {
		// dentry is COW, so no need to lock it
		dentry := item.(*Dentry)
		if saveAll || dentry.IsDirty() {
			log.LogCriticalf("DEBUG: Part(%v) save dentry [%s] to dentry cf", mp.config.PartitionId, dentry.Name)
			key := []byte(fmt.Sprintf("%d_%s", dentry.ParentId, dentry.Name))
			if data, err = dentry.Marshal(); err != nil {
				return false
			}
			if err = mp.metaDB.db.PutCF(cf, key, data, false); err != nil {
				return false
			}
			cnt++
		}
		return true
	}

	sm.dentryTree.Ascend(dentryHandler)

	log.LogInfof("storeDentryToDB: store complete: partitoinID(%v) volume(%v) numDentries(%v) dirtyDentries(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.dentryTree.Len(), cnt)
	log.LogCriticalf("DEBUG: storeDentryToDB: store complete: partitoinID(%v) volume(%v) numDentries(%v) dirtyDentries(%v) saveAll(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.dentryTree.Len(), cnt, saveAll)

	return
}

func (mp *metaPartition) loadDentryFromDB(dir string) (err error) {
	var (
		cf    *gorocksdb.ColumnFamilyHandle
		found bool
		nr    uint64
	)

	if _, err = os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return
	}

	if cf, found = mp.metaDB.cfs["dentry"]; !found {
		err = fmt.Errorf("dentry column family not exist")
		log.LogError(err)
		log.LogCriticalf("DEBUG: Part(%v): %v", mp.config.RootDir, err)
		return
	}

	db := mp.metaDB.db

	// FIXME: load nothing?
	// FIXME: or load dentries accroding to inode LRU?
	loadDentryFunc := func(k, v *gorocksdb.Slice) error {
		dentry := &Dentry{}
		if e := dentry.Unmarshal(v.Data()); e != nil {
			return errors.NewErrorf("[loadDentryFromDB] Unmarshal: %v", e)
		}

		log.LogCriticalf("DEBUG: [loadDentryFromDB] Part(%v) load dentry(%v)", mp.config.PartitionId, dentry.Name)

		if status := mp.fsmCreateDentry(dentry, true); status != proto.OpOk {
			return errors.NewErrorf("[loadDentryFromDB] createDentry dentry: %v, resp code: %d", dentry, status)
		}
		return nil
	}
	if nr, err = db.LoadNCF(cf, 0, loadDentryFunc, nil); err != nil {
		log.LogCriticalf("DEBUG: [loadDentryFromDB] Part(%v) load: %v", mp.config.RootDir, err)
		panic(err)
	}

	log.LogInfof("loadDentryFromDB: load complete: partitonID(%v) volume(%v) numDentries(%v)",
		mp.config.PartitionId, mp.config.VolName, nr)
	log.LogCriticalf("DEBUG: loadDentryFromDB: load complete: partitonID(%v) volume(%v) numDentries(%v)",
		mp.config.PartitionId, mp.config.VolName, nr)

	return
}

func (mp *metaPartition) storeExtendToDB(sm *storeMsg) (err error) {
	var (
		data    []byte
		saveAll bool
		cnt     uint64
	)

	cf, found := mp.metaDB.cfs["extend"]
	if !found {
		err = errors.New("extend column family not found")
		return
	}

	// save all dirty extends
	// if it is the first time to save extends to DB, save them all
	if mp.metaDB.TestStatus(MetaDBNew) {
		saveAll = true
	}
	extendHandler := func(item btree.Item) bool {
		// extend is COW, so no need to lock it
		extend := item.(*Extend)
		if saveAll || extend.IsDirty() {
			key := []byte(fmt.Sprintf("%v", extend.inode))
			if data, err = extend.Bytes(); err != nil {
				return false
			}
			if err = mp.metaDB.db.PutCF(cf, key, data, false); err != nil {
				return false
			}
			cnt++
		}
		return true
	}

	sm.extendTree.Ascend(extendHandler)

	log.LogInfof("storeExtendToDB: store complete: partitoinID(%v) volume(%v) numExtends(%v) dirtyExtends(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.extendTree.Len(), cnt)

	return
}

func (mp *metaPartition) loadExtendFromDB(dir string) (err error) {
	var (
		cf    *gorocksdb.ColumnFamilyHandle
		found bool
		nr    uint64
	)

	if _, err = os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return
	}

	if cf, found = mp.metaDB.cfs["extend"]; !found {
		err = fmt.Errorf("extend column family not exist")
		log.LogError(err)
		return
	}

	db := mp.metaDB.db

	loadExtendFunc := func(k, v *gorocksdb.Slice) error {
		extend, e := NewExtendFromBytes(v.Data())
		if e != nil {
			return errors.NewErrorf("[loadExtendFromDB] Unmarshal: %v", e)
		}

		_ = mp.fsmSetXAttr(extend)
		return nil
	}
	if nr, err = db.LoadNCF(cf, 0, loadExtendFunc, nil); err != nil {
		panic(err)
	}

	log.LogInfof("loadExtendFromDB: load complete: partitionID(%v) volume(%v) numExtends(%v)",
		mp.config.PartitionId, mp.config.VolName, nr)

	return
}

func (mp *metaPartition) storeMultipartToDB(sm *storeMsg) (err error) {
	var (
		data    []byte
		saveAll bool
		cnt     uint64
	)

	cf, found := mp.metaDB.cfs["multipart"]
	if !found {
		err = errors.New("multipart column family not found")
		return
	}

	// save all dirty multiparts
	// if it is the first time to save multiparts to DB, save them all
	if mp.metaDB.TestStatus(MetaDBNew) {
		saveAll = true
	}
	multipartHandler := func(item btree.Item) bool {
		// multipart is COW, so no need to lock it
		multipart := item.(*Multipart)
		if saveAll || multipart.IsDirty() {
			key := []byte(fmt.Sprintf("%s_%d", multipart.key, multipart.id))
			if data, err = multipart.Bytes(); err != nil {
				return false
			}
			if err = mp.metaDB.db.PutCF(cf, key, data, false); err != nil {
				return false
			}
			cnt++
		}
		return true
	}

	sm.multipartTree.Ascend(multipartHandler)

	log.LogInfof("storeMultipartToDB: store complete: partitoinID(%v) volume(%v) numMultiparts(%v) dirtyMultiparts(%v)",
		mp.config.PartitionId, mp.config.VolName, sm.multipartTree.Len(), cnt)

	return
}

func (mp *metaPartition) loadMultipartFromDB(dir string) (err error) {
	var (
		cf    *gorocksdb.ColumnFamilyHandle
		found bool
		nr    uint64
	)

	if _, err = os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return
	}

	if cf, found = mp.metaDB.cfs["multipart"]; !found {
		err = fmt.Errorf("multipart column family not exist")
		log.LogError(err)
		return
	}

	db := mp.metaDB.db

	loadMultipartFunc := func(k, v *gorocksdb.Slice) error {
		multipart := MultipartFromBytes(v.Data())
		_ = mp.fsmCreateMultipart(multipart)
		return nil
	}
	if nr, err = db.LoadNCF(cf, 0, loadMultipartFunc, nil); err != nil {
		panic(err)
	}

	log.LogInfof("loadMultipartFromDB: load complete: partitionID(%v) numMultiparts(%v)",
		mp.config.PartitionId, nr)

	return
}
