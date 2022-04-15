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
	"encoding/json"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tecbot/gorocksdb"
	raftproto "github.com/tiglabs/raft/proto"
)

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
)

// Errors
var (
	ErrInodeIDOutOfRange = errors.New("inode ID out of range")
)

type sortedPeers []proto.Peer

func (sp sortedPeers) Len() int {
	return len(sp)
}
func (sp sortedPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortedPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

// MetaPartitionConfig is used to create a meta partition.
type MetaPartitionConfig struct {
	// Identity for raftStore group. RaftStore nodes in the same raftStore group must have the same groupID.
	PartitionId uint64              `json:"partition_id"`
	VolName     string              `json:"vol_name"`
	Start       uint64              `json:"start"` // Minimal Inode ID of this range. (Required during initialization)
	End         uint64              `json:"end"`   // Maximal Inode ID of this range. (Required during initialization)
	Peers       []proto.Peer        `json:"peers"` // Peers information of the raftStore
	Cursor      uint64              `json:"-"`     // Cursor ID of the inode that have been assigned
	NodeId      uint64              `json:"-"`
	RootDir     string              `json:"-"`
	BeforeStart func()              `json:"-"`
	AfterStart  func()              `json:"-"`
	BeforeStop  func()              `json:"-"`
	AfterStop   func()              `json:"-"`
	RaftStore   raftstore.RaftStore `json:"-"`
	ConnPool    *util.ConnectPool   `json:"-"`
}

func (c *MetaPartitionConfig) checkMeta() (err error) {
	if c.PartitionId <= 0 {
		err = errors.NewErrorf("[checkMeta]: partition id at least 1, "+
			"now partition id is: %d", c.PartitionId)
		return
	}
	if c.Start < 0 {
		err = errors.NewErrorf("[checkMeta]: start at least 0")
		return
	}
	if c.End <= c.Start {
		err = errors.NewErrorf("[checkMeta]: end=%v, "+
			"start=%v; end <= start", c.End, c.Start)
		return
	}
	if len(c.Peers) <= 0 {
		err = errors.NewErrorf("[checkMeta]: must have peers, now peers is 0")
		return
	}
	return
}

func (c *MetaPartitionConfig) sortPeers() {
	sp := sortedPeers(c.Peers)
	sort.Sort(sp)
}

// OpInode defines the interface for the inode operations.
type OpInode interface {
	CreateInode(req *CreateInoReq, p *Packet) (err error)
	UnlinkInode(req *UnlinkInoReq, p *Packet) (err error)
	UnlinkInodeBatch(req *BatchUnlinkInoReq, p *Packet) (err error)
	InodeGet(req *InodeGetReq, p *Packet) (err error)
	InodeGetBatch(req *InodeGetReqBatch, p *Packet) (err error)
	CreateInodeLink(req *LinkInodeReq, p *Packet) (err error)
	EvictInode(req *EvictInodeReq, p *Packet) (err error)
	EvictInodeBatch(req *BatchEvictInodeReq, p *Packet) (err error)
	SetAttr(reqData []byte, p *Packet) (err error)
	WalkInodeTree(handler func(item btree.Item) bool)
	DeleteInode(req *proto.DeleteInodeRequest, p *Packet) (err error)
	DeleteInodeBatch(req *proto.DeleteInodeBatchRequest, p *Packet) (err error)
}

type OpExtend interface {
	SetXAttr(req *proto.SetXAttrRequest, p *Packet) (err error)
	GetXAttr(req *proto.GetXAttrRequest, p *Packet) (err error)
	BatchGetXAttr(req *proto.BatchGetXAttrRequest, p *Packet) (err error)
	RemoveXAttr(req *proto.RemoveXAttrRequest, p *Packet) (err error)
	ListXAttr(req *proto.ListXAttrRequest, p *Packet) (err error)
	UpdateSummaryInfo(req *proto.UpdateSummaryInfoRequest, p *Packet) (err error)
}

// OpDentry defines the interface for the dentry operations.
type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet) (err error)
	DeleteDentryBatch(req *BatchDeleteDentryReq, p *Packet) (err error)
	UpdateDentry(req *UpdateDentryReq, p *Packet) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	ReadDirLimit(req *ReadDirLimitReq, p *Packet) (err error)
	ReadDirOnly(req *ReadDirOnlyReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
	WalkDentryTree(handler func(item btree.Item) bool)
}

// OpExtent defines the interface for the extent operations.
type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentAppendWithCheck(req *proto.AppendExtentKeyWithCheckRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
	ExtentsTruncate(req *ExtentsTruncateReq, p *Packet) (err error)
	BatchExtentAppend(req *proto.AppendExtentKeysRequest, p *Packet) (err error)
}

type OpMultipart interface {
	GetMultipart(req *proto.GetMultipartRequest, p *Packet) (err error)
	CreateMultipart(req *proto.CreateMultipartRequest, p *Packet) (err error)
	AppendMultipart(req *proto.AddMultipartPartRequest, p *Packet) (err error)
	RemoveMultipart(req *proto.RemoveMultipartRequest, p *Packet) (err error)
	ListMultipart(req *proto.ListMultipartRequest, p *Packet) (err error)
}

// OpMeta defines the interface for the metadata operations.
type OpMeta interface {
	OpInode
	OpDentry
	OpExtent
	OpPartition
	OpExtend
	OpMultipart
}

// OpPartition defines the interface for the partition operations.
type OpPartition interface {
	IsLeader() (leaderAddr string, isLeader bool)
	GetCursor() uint64
	GetBaseConfig() MetaPartitionConfig
	ResponseLoadMetaPartition(p *Packet) (err error)
	PersistMetadata() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	Reset() (err error)
	UpdatePartition(req *UpdatePartitionReq, resp *UpdatePartitionResp) (err error)
	DeleteRaft() error
	IsExsitPeer(peer proto.Peer) bool
	TryToLeader(groupID uint64) error
	CanRemoveRaftMember(peer proto.Peer) error
	IsEquareCreateMetaPartitionRequst(request *proto.CreateMetaPartitionRequest) (err error)
}

// MetaPartition defines the interface for the meta partition operations.
type MetaPartition interface {
	Start() error
	Stop()
	OpMeta
	LoadSnapshot(path string) error
	ForceSetMetaPartitionToLoadding()
	ForceSetMetaPartitionToFininshLoad()
}

const (
	MetaDBInit  uint32 = 0x0
	MetaDBReady uint32 = 0x1
	MetaDBClose uint32 = 0x4
	MetaDBNew   uint32 = 0x8 // DB is new until the first checkpoint is done

	DBLRUCacheSize    = 1000
	DBWriteBufferSize = 4 * util.MB
	DBLogSizeForFlush = 128 * util.MB
	ColdTimeMax       = time.Minute //time.Hour
	AccessTimeMax     = 60 //24 * 3600 // unit: second
	ReclaimInterval   = time.Minute
	ReclaimStartCnt   = 1 //100
	ReclaimLimit      = 10
	ReclaimScanLimit  = 100
)

type MetaDB struct {
	// persist inode/dentry/extentd/multipart
	db     *raftstore.RocksDBStore
	cfs    map[string]*gorocksdb.ColumnFamilyHandle
	status uint32 // atomic value

	stopC chan struct{}

	snapshotLock sync.Mutex
}

// metaPartition manages the range of the inode IDs.
// When a new inode is requested, it allocates a new inode id for this inode if possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config                 *MetaPartitionConfig
	size                   uint64 // For partition all file size
	applyID                uint64 // Inode/Dentry max applyID, this index will be update after restoring from the dumped data.
	dentryTree             *Btree
	inodeTree              *Btree // btree for inodes
	extendTree             *Btree // btree for inode extend (XAttr) management
	multipartTree          *Btree // collection for multipart management
	raftPartition          raftstore.Partition
	stopC                  chan bool
	storeChan              chan *storeMsg
	state                  uint32
	delInodeFp             *os.File
	freeList               *freeList // free inode list
	extDelCh               chan []proto.ExtentKey
	extReset               chan struct{}
	vol                    *Vol
	manager                *metadataManager
	isLoadingMetaPartition bool
	summaryLock            sync.Mutex

	metaDB *MetaDB
}

func (mp *metaPartition) ForceSetMetaPartitionToLoadding() {
	mp.isLoadingMetaPartition = true
}

func (mp *metaPartition) ForceSetMetaPartitionToFininshLoad() {
	mp.isLoadingMetaPartition = false
}

// Start starts a meta partition.
func (mp *metaPartition) Start() (err error) {
	if atomic.CompareAndSwapUint32(&mp.state, common.StateStandby, common.StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = common.StateStandby
			} else {
				newState = common.StateRunning
			}
			atomic.StoreUint32(&mp.state, newState)
		}()
		if mp.config.BeforeStart != nil {
			mp.config.BeforeStart()
		}
		if err = mp.onStart(); err != nil {
			err = errors.NewErrorf("[Start]->%s", err.Error())
			return
		}
		if mp.config.AfterStart != nil {
			mp.config.AfterStart()
		}
	}
	return
}

// Stop stops a meta partition.
func (mp *metaPartition) Stop() {
	if atomic.CompareAndSwapUint32(&mp.state, common.StateRunning, common.StateShutdown) {
		defer atomic.StoreUint32(&mp.state, common.StateStopped)
		if mp.config.BeforeStop != nil {
			mp.config.BeforeStop()
		}
		mp.onStop()
		if mp.config.AfterStop != nil {
			mp.config.AfterStop()
			log.LogDebugf("[AfterStop]: partition id=%d execute ok.",
				mp.config.PartitionId)
		}
	}
}

func (mp *metaPartition) onStart() (err error) {
	defer func() {
		if err == nil {
			return
		}
		mp.onStop()
	}()
	if err = mp.load(); err != nil {
		err = errors.NewErrorf("[onStart]:load partition id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	mp.startSchedule(mp.applyID)
	if err = mp.startFreeList(); err != nil {
		err = errors.NewErrorf("[onStart] start free list id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if err = mp.startRaft(); err != nil {
		err = errors.NewErrorf("[onStart]start raft id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	return
}

func (mp *metaPartition) onStop() {
	mp.releaseMetaDB()
	mp.stopRaft()
	mp.stop()
	if mp.delInodeFp != nil {
		mp.delInodeFp.Sync()
		mp.delInodeFp.Close()
	}
}

func (mp *metaPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range mp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftproto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicaPort:   replicaPort,
		}
		peers = append(peers, rp)
	}
	log.LogDebugf("start partition id=%d raft peers: %s",
		mp.config.PartitionId, peers)
	pc := &raftstore.PartitionConfig{
		ID:      mp.config.PartitionId,
		Applied: mp.applyID,
		Peers:   peers,
		SM:      mp,
	}
	mp.raftPartition, err = mp.config.RaftStore.CreatePartition(pc)
	if err == nil {
		mp.ForceSetMetaPartitionToFininshLoad()
	}
	return
}

func (mp *metaPartition) stopRaft() {
	if mp.raftPartition != nil {
		// TODO Unhandled errors
		//mp.raftPartition.Stop()
	}
	return
}

func (mp *metaPartition) getRaftPort() (heartbeat, replica int, err error) {
	raftConfig := mp.config.RaftStore.RaftConfig()
	heartbeatAddrSplits := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicaAddrSplits := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrSplits) != 2 {
		err = ErrIllegalHeartbeatAddress
		return
	}
	if len(replicaAddrSplits) != 2 {
		err = ErrIllegalReplicateAddress
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrSplits[1])
	if err != nil {
		return
	}
	replica, err = strconv.Atoi(replicaAddrSplits[1])
	if err != nil {
		return
	}
	return
}

// NewMetaPartition creates a new meta partition with the specified configuration.
func NewMetaPartition(conf *MetaPartitionConfig, manager *metadataManager) (MetaPartition, error) {
	mp := &metaPartition{
		config:        conf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, 10000),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
	}

	if err := mp.newMetaDB(conf); err != nil {
		return nil, err
	}

	return mp, nil
}

// IsLeader returns the raft leader address and if the current meta partition is the leader.
func (mp *metaPartition) IsLeader() (leaderAddr string, ok bool) {
	if mp.raftPartition == nil {
		return
	}
	leaderID, _ := mp.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	ok = leaderID == mp.config.NodeId
	for _, peer := range mp.config.Peers {
		if leaderID == peer.ID {
			leaderAddr = peer.Addr
			return
		}
	}
	return
}

func (mp *metaPartition) GetPeers() (peers []string) {
	peers = make([]string, 0)
	for _, peer := range mp.config.Peers {
		if mp.config.NodeId == peer.ID {
			continue
		}
		peers = append(peers, peer.Addr)
	}
	return
}

// GetCursor returns the cursor stored in the config.
func (mp *metaPartition) GetCursor() uint64 {
	return atomic.LoadUint64(&mp.config.Cursor)
}

// PersistMetadata is the wrapper of persistMetadata.
func (mp *metaPartition) PersistMetadata() (err error) {
	mp.config.sortPeers()
	err = mp.persistMetadata()
	return
}

func (mp *metaPartition) LoadSnapshot(rootDir string) (err error) {
	if err = mp.loadInode(rootDir); err != nil {
		return
	}
	if err = mp.loadDentry(rootDir); err != nil {
		return
	}
	if err = mp.loadExtend(rootDir); err != nil {
		return
	}
	if err = mp.loadMultipart(rootDir); err != nil {
		return
	}
	if err = mp.loadApplyID(rootDir); err != nil {
		return
	}
	return
}

func (mp *metaPartition) load() (err error) {
	if err = mp.loadMetadata(); err != nil {
		return
	}
	if err = mp.loadInode(mp.config.RootDir); err != nil {
		return
	}
	if err = mp.loadDentry(mp.config.RootDir); err != nil {
		return
	}
	if err = mp.loadExtend(mp.config.RootDir); err != nil {
		return
	}
	if err = mp.loadMultipart(mp.config.RootDir); err != nil {
		return
	}
	if err = mp.loadApplyID(mp.config.RootDir); err != nil {
		return
	}
	mp.metaDB.SetStatus(MetaDBReady)
	return
}

func (mp *metaPartition) store(sm *storeMsg) (err error) {
	tmpDir := path.Join(mp.config.RootDir, snapshotDirTmp)
	if _, err = os.Stat(tmpDir); err == nil {
		// TODO Unhandled errors
		os.RemoveAll(tmpDir)
	}
	err = nil
	if err = os.MkdirAll(tmpDir, 0775); err != nil {
		return
	}

	defer func() {
		if err != nil {
			// TODO Unhandled errors
			os.RemoveAll(tmpDir)
		}
	}()
	var crcBuffer = bytes.NewBuffer(make([]byte, 0, 16))
	var storeFuncs = []func(dir string, sm *storeMsg) (uint32, error){
		mp.storeInode,
		mp.storeDentry,
		mp.storeExtend,
		mp.storeMultipart,
	}
	for _, storeFunc := range storeFuncs {
		var crc uint32
		if crc, err = storeFunc(tmpDir, sm); err != nil {
			return
		}
		if crcBuffer.Len() != 0 {
			crcBuffer.WriteString(" ")
		}
		crcBuffer.WriteString(fmt.Sprintf("%d", crc))
	}
	if err = mp.storeApplyID(tmpDir, sm); err != nil {
		return
	}
	// write crc to file
	if err = ioutil.WriteFile(path.Join(tmpDir, SnapshotSign), crcBuffer.Bytes(), 0775); err != nil {
		return
	}
	snapshotDir := path.Join(mp.config.RootDir, snapshotDir)
	// check snapshot backup
	backupDir := path.Join(mp.config.RootDir, snapshotBackup)
	if _, err = os.Stat(backupDir); err == nil {
		if err = os.RemoveAll(backupDir); err != nil {
			return
		}
	}
	err = nil

	// rename snapshot
	if _, err = os.Stat(snapshotDir); err == nil {
		if err = os.Rename(snapshotDir, backupDir); err != nil {
			return
		}
	}
	err = nil

	if err = os.Rename(tmpDir, snapshotDir); err != nil {
		_ = os.Rename(backupDir, snapshotDir)
		return
	}
	err = os.RemoveAll(backupDir)
	return
}

// UpdatePeers updates the peers.
func (mp *metaPartition) UpdatePeers(peers []proto.Peer) {
	mp.config.Peers = peers
}

// DeleteRaft deletes the raft partition.
func (mp *metaPartition) DeleteRaft() (err error) {
	err = mp.raftPartition.Delete()
	return
}

// Return a new inode ID and update the offset.
func (mp *metaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := atomic.LoadUint64(&mp.config.Cursor)
		end := mp.config.End
		if cur >= end {
			return 0, ErrInodeIDOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.config.Cursor, cur, newId) {
			return newId, nil
		}
	}
}

// ChangeMember changes the raft member with the specified one.
func (mp *metaPartition) ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = mp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

// GetBaseConfig returns the configuration stored in the meta partition. TODO remove? no usage?
func (mp *metaPartition) GetBaseConfig() MetaPartitionConfig {
	return *mp.config
}

// UpdatePartition updates the meta partition. TODO remove? no usage?
func (mp *metaPartition) UpdatePartition(req *UpdatePartitionReq,
	resp *UpdatePartitionResp) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	r, err := mp.submit(opFSMUpdatePartition, reqData)
	if err != nil {
		resp.Status = proto.TaskFailed
		resp.Result = err.Error()
		return
	}
	if status := r.(uint8); status != proto.OpOk {
		resp.Status = proto.TaskFailed
		p := &Packet{}
		p.ResultCode = status
		err = errors.NewErrorf("[UpdatePartition]: %s", p.GetResultMsg())
		resp.Result = p.GetResultMsg()
	}
	resp.Status = proto.TaskSucceeds
	return
}

func (mp *metaPartition) DecommissionPartition(req []byte) (err error) {
	_, err = mp.submit(opFSMDecommissionPartition, req)
	return
}

func (mp *metaPartition) IsExsitPeer(peer proto.Peer) bool {
	for _, hasExsitPeer := range mp.config.Peers {
		if hasExsitPeer.Addr == peer.Addr && hasExsitPeer.ID == peer.ID {
			return true
		}
	}
	return false
}

func (mp *metaPartition) TryToLeader(groupID uint64) error {
	return mp.raftPartition.TryToLeader(groupID)
}

// ResponseLoadMetaPartition loads the snapshot signature. TODO remove? no usage?
func (mp *metaPartition) ResponseLoadMetaPartition(p *Packet) (err error) {
	resp := &proto.MetaPartitionLoadResponse{
		PartitionID: mp.config.PartitionId,
		DoCompare:   true,
	}
	resp.MaxInode = mp.GetCursor()
	resp.InodeCount = uint64(mp.inodeTree.Len())
	resp.DentryCount = uint64(mp.dentryTree.Len())
	resp.ApplyID = mp.applyID
	if err != nil {
		err = errors.Trace(err,
			"[ResponseLoadMetaPartition] check snapshot")
		return
	}

	data, err := json.Marshal(resp)
	if err != nil {
		err = errors.Trace(err, "[ResponseLoadMetaPartition] marshal")
		return
	}
	p.PacketOkWithBody(data)
	return
}

// MarshalJSON is the wrapper of json.Marshal.
func (mp *metaPartition) MarshalJSON() ([]byte, error) {
	return json.Marshal(mp.config)
}

// TODO remove? no usage?
// Reset resets the meta partition.
func (mp *metaPartition) Reset() (err error) {
	mp.inodeTree.Reset()
	mp.dentryTree.Reset()
	mp.config.Cursor = 0
	mp.applyID = 0

	// remove files
	filenames := []string{applyIDFile, dentryFile, inodeFile, extendFile, multipartFile}
	for _, filename := range filenames {
		filepath := path.Join(mp.config.RootDir, filename)
		if err = os.Remove(filepath); err != nil {
			return
		}
	}

	return
}

//
func (mp *metaPartition) canRemoveSelf() (canRemove bool, err error) {
	var partition *proto.MetaPartitionInfo
	if partition, err = masterClient.ClientAPI().GetMetaPartition(mp.config.PartitionId); err != nil {
		log.LogErrorf("action[canRemoveSelf] err[%v]", err)
		return
	}
	canRemove = false
	var existInPeers bool
	for _, peer := range partition.Peers {
		if mp.config.NodeId == peer.ID {
			existInPeers = true
		}
	}
	if !existInPeers {
		canRemove = true
		return
	}
	if mp.config.NodeId == partition.OfflinePeerID {
		canRemove = true
		return
	}
	return
}

func (db *MetaDB) SetStatus(st uint32) {
	for {
		oldSt := atomic.LoadUint32(&db.status)
		newSt := oldSt | st
		if atomic.CompareAndSwapUint32(&db.status, oldSt, newSt) {
			break
		}
	}
}

func (db *MetaDB) ClearStatus(st uint32) {
	for {
		oldSt := atomic.LoadUint32(&db.status)
		newSt := oldSt ^ st
		if atomic.CompareAndSwapUint32(&db.status, oldSt, newSt) {
			break
		}
	}
}

func (db *MetaDB) TestStatus(st uint32) bool {
	currSt := atomic.LoadUint32(&db.status)
	if currSt&st == 0 {
		return false
	}
	return true
}

func cleanupStaleDBData(rootDir string, victimName string) error {
	victimDir := path.Join(rootDir, victimName)
	expName := fmt.Sprintf("%s%v", CheckpointDirExpPrefix, time.Now().UnixNano())
	expPath := path.Join(rootDir, expName)
	if err := os.Rename(victimDir, expPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// there may have several directories related checkpoint:
//  * 'snapshot': has the highest priority, if it exists, load it, and ignore
//                other directories
//  * 'metaDB': is the workspace of rocksdb, it may contains newer data compared
//              with 'checkpoint'. So when starting up a mp, data in 'metaDB'
//              should not be used, and 'metaDB' should be rolled back to
//              'checkpoint'
//  * 'checkpoint': is the latest applied data, it has a higher priority than
//                  'checkpoint.old'
//  * 'checkpoint.old': is a backup for 'checkpoint'
//  * 'checkpoint.new': is an unfinished checkpoint, should not be used
//  * 'expired_checkpoint_xxx': contains stale data, should be deleted
//
// In one word, only 'checkpoint' or 'checkpoint.old' could be used.
//
// remove 'metaDB' && 'checkpoint.new'
//         |
//         v
// check if 'checkpoint' exist
//       Y /               \ N
//         |             check if 'checkpoint.old' exist
//         |                          Y /          \ N
//         |                rename to 'checkpoint' |
//         v                           |           |
// remove 'checkpoint.old' <-----------'           |
//         |                                       |
//         v                                       |
// create checkpoint at 'metaDB'                   |
//         |                                       |
//         v                                       |
// remove 'checkpoint.old'                         |
//         |                                       |
//         v                                       |
// open DB in 'metaDB' <---------------------------'
func (mp *metaPartition) newMetaDB(conf *MetaPartitionConfig) (err error) {
	var cfs []*gorocksdb.ColumnFamilyHandle

	// "default" is not used, but required by rocksdb
	cfNames := []string{"default", "inode", "orphaninode", "dentry", "extend", "multipart"}
	metaDB := &MetaDB{
		cfs:    make(map[string]*gorocksdb.ColumnFamilyHandle),
		status: MetaDBInit,
		stopC:  make(chan struct{}),
	}

	// cleanup 'metaDB' & 'checkpoint.new'
	log.LogCriticalf("DEBUG: cleanup Part(%v) [%s]", mp.config.PartitionId, metaDBDir)
	err = cleanupStaleDBData(conf.RootDir, metaDBDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	log.LogCriticalf("DEBUG: cleanup Part(%v) [%s]", mp.config.PartitionId, NewCheckpointDir)
	err = cleanupStaleDBData(conf.RootDir, NewCheckpointDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// find 'checkpoint' or 'checkpoint.old'
	cpDir := path.Join(conf.RootDir, CheckpointDir)
	st, err := os.Stat(cpDir)
	if (err != nil && !os.IsNotExist(err)) || (st != nil && !st.IsDir()) {
		err = fmt.Errorf("Invalid checkpoint dir[%s]: %v", cpDir, err)
		log.LogError(err)
		return err
	}

	if os.IsNotExist(err) {
		log.LogCriticalf("DEBUG: Part(%v) %v not exist check %v", mp.config.PartitionId, CheckpointDir, OldCheckpointDir)
		cpOldDir := path.Join(conf.RootDir, OldCheckpointDir)
		st, err = os.Stat(cpOldDir)
		if (err != nil && !os.IsNotExist(err)) || (st != nil && !st.IsDir()) {
			err = fmt.Errorf("Invalid checkpoint dir[%s]: %v", cpOldDir, err)
			log.LogError(err)
			log.LogCriticalf("DEBUG: Part(%v): %v", mp.config.PartitionId, err)
			return err
		}
		if err == nil {
			log.LogCriticalf("DEBUG: Part(%v) %v exist rename to %v", mp.config.PartitionId, cpOldDir, cpDir)
			if e := os.Rename(cpOldDir, cpDir); e != nil {
				err = fmt.Errorf("Unable to rename [%s] -> [%s]: %v", cpOldDir, cpDir, e)
				log.LogError(err)
				return err
			}
		}
	}

	// clear 'checkpoint.old'
	log.LogCriticalf("DEBUG: cleanup Part(%v) [%s]", mp.config.PartitionId, OldCheckpointDir)
	if e := cleanupStaleDBData(conf.RootDir, OldCheckpointDir); e != nil && !os.IsNotExist(err) {
		log.LogErrorf("Failed to cleanup partition[%v] [%s]: %v", mp.config.PartitionId, OldCheckpointDir, e)
		log.LogCriticalf("DEBUG: Part(%v) Failed to cleanup [%s]: %v", mp.config.PartitionId, OldCheckpointDir, e)
		return e
	}

	workDir := path.Join(conf.RootDir, metaDBDir)
	if err == nil {
		// keep the current checkpoint until new checkpoint is create
		log.LogCriticalf("DEBUG: open DB %s to create checkpoint at %s", cpDir, workDir)
		store, cfs, err := raftstore.OpenWithColumnFamilies(cpDir, cfNames, DBLRUCacheSize, DBWriteBufferSize)
		if err != nil {
			log.LogErrorf("Failed to create or open DB in [%s]: %v", cpDir, err)
			log.LogCriticalf("DEBUG: Part(%v) Failed to create or open DB in [%s]: %v", mp.config.PartitionId, cpDir, err)
			return err
		}
		cp, err := store.NewCheckpoint()
		if err != nil {
			log.LogErrorf("Failed to new checkpoint: %v", err)
			log.LogCriticalf("DEBUG: Part(%v) Failed to new checkpoint: %v", mp.config.PartitionId, err)
			for _, cf := range cfs {
				cf.Destroy()
			}
			store.Close()
			return err
		}
		if err = cp.CreateCheckpoint(workDir, 0); err != nil {
			log.LogError("Failed to rollback to checkpoint: %v", err)
			log.LogCriticalf("DEBUG: Part(%v) Failed to rollback to checkpoint: %v", mp.config.PartitionId, err)
			cp.Destroy()
			for _, cf := range cfs {
				cf.Destroy()
			}
			store.Close()
			os.RemoveAll(workDir)
			return err
		}
		cp.Destroy()
		for _, cf := range cfs {
			cf.Destroy()
		}
		store.Close()
	}

	log.LogCriticalf("DEBUG: open DB at %s", workDir)
	metaDB.db, cfs, err = raftstore.OpenWithColumnFamilies(
		workDir, cfNames, DBLRUCacheSize, DBWriteBufferSize)
	if err != nil {
		if strings.Contains(err.Error(), "Column family not found") {
			log.LogCriticalf("DEBUG: Part(%v) Failed to open DB with column families in [%s], create it alone: %v", mp.config.PartitionId, workDir, err)
			// column family not exist, create them
			metaDB.db, err = raftstore.NewRocksDBStore(workDir, DBLRUCacheSize, DBWriteBufferSize)
			if err == nil {
				cfs = make([]*gorocksdb.ColumnFamilyHandle, len(cfNames))
				for i, n := range cfNames[1:] {
					log.LogCriticalf("DEBUG: Part(%v) create column family [%s]", mp.config.PartitionId, n)
					if cfs[i+1], err = metaDB.db.CreateColumnFamily(n); err != nil {
						break
					}
				}
			}
			metaDB.SetStatus(MetaDBNew)
		}
		if err != nil {
			for _, cf := range cfs {
				if cf != nil {
					// no need to drop cf, the hole directory will be removed
					cf.Destroy()
				}
			}
			metaDB.db.Close()
			log.LogCriticalf("DEBUG: Part(%v) Failed to create or open DB in [%s]: %v", mp.config.PartitionId, workDir, err)
			return err
		}
	}

	for i, n := range cfNames {
		metaDB.cfs[n] = cfs[i]
	}

	mp.metaDB = metaDB

	go mp.ReclaimColdInodes()
	go mp.RemoveExpiredCheckpoints()

	return nil
}

func (mp *metaPartition) releaseMetaDB() {
	if mp.metaDB == nil || mp.metaDB.db == nil {
		return
	}

	mp.metaDB.ClearStatus(MetaDBReady)
	mp.metaDB.SetStatus(MetaDBClose)
	close(mp.metaDB.stopC)
	// wait snapshot to finish if there is a snapshot is wroten
	mp.metaDB.snapshotLock.Lock()
	for n, cf := range mp.metaDB.cfs {
		delete(mp.metaDB.cfs, n)
		cf.Destroy()
	}
	mp.metaDB.snapshotLock.Unlock()

	mp.metaDB.db.Close()
}

func (mp *metaPartition) storeToDB(sm *storeMsg) (err error) {
	var cp *gorocksdb.Checkpoint

	log.LogCriticalf("DEBUG: Part(%v) start store to snapshot", mp.config.PartitionId)
	// cleanup
	newCPDir := path.Join(mp.config.RootDir, NewCheckpointDir)
	if _, err = os.Stat(newCPDir); err == nil {
		if err = os.RemoveAll(newCPDir); err != nil {
			return
		}
	}

	// wait another snapshot or writeback to finish
	mp.metaDB.snapshotLock.Lock()
	defer func() {
		mp.metaDB.snapshotLock.Unlock()
		if err == nil {
			mp.metaDB.db.Flush()
		} else {
			os.RemoveAll(newCPDir)
		}
	}()

	// save dirty data to DB
	var storeFuncs = []func(sm *storeMsg) error{
		mp.storeInodeToDB,
		mp.storeDentryToDB,
		mp.storeExtendToDB,
		mp.storeMultipartToDB,
	}
	for _, storeFunc := range storeFuncs {
		if err = storeFunc(sm); err != nil {
			return
		}
	}

	// do new checkpoint and remove old one by the following steps:
	//  1. mkdir 'checkpoint.new' and do checkpoint in this directory
	//  2. mv 'checkpoint' to 'checkpoint.old'
	//  3. mv 'checkpoint.new' to 'checkpoint'
	//  4. mv 'checkpoint.old' to 'expired_checkpoint'
	if cp, err = mp.metaDB.db.NewCheckpoint(); err != nil {
		log.LogErrorf("Failed to new checkpoint: %v", err)
		return
	}
	defer cp.Destroy()
	if err = cp.CreateCheckpoint(newCPDir, DBLogSizeForFlush); err != nil {
		return
	}

	if err = mp.storeApplyID(newCPDir, sm); err != nil {
		return
	}

	origCPDir := path.Join(mp.config.RootDir, CheckpointDir)
	oldCPDir := path.Join(mp.config.RootDir, OldCheckpointDir)
	// origCPDir does not exist if it's the a new DB
	if err = os.Rename(origCPDir, oldCPDir); err != nil && !os.IsNotExist(err) {
		return
	}

	if err = os.Rename(newCPDir, origCPDir); err != nil {
		return
	}

	expiredDir := fmt.Sprintf("%s%v", CheckpointDirExpPrefix, time.Now().UnixNano())
	expiredCPDir := path.Join(mp.config.RootDir, expiredDir)
	// oldCPDir does not exist if origCPDir does not exist
	if err = os.Rename(oldCPDir, expiredCPDir); err != nil && !os.IsNotExist(err) {
		return
	}

	// remove old snapshot related directories
	os.RemoveAll(path.Join(mp.config.RootDir, snapshotDir))
	os.RemoveAll(path.Join(mp.config.RootDir, snapshotDirTmp))
	os.RemoveAll(path.Join(mp.config.RootDir, snapshotBackup))

	// clear MetaDBNew
	log.LogCriticalf("DEBUG: Part(%v) clear MetaDBNew", mp.config.PartitionId)
	mp.metaDB.ClearStatus(MetaDBNew)

	return nil
}

func (mp *metaPartition) RemoveExpiredCheckpoints() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			if !mp.metaDB.TestStatus(MetaDBReady) {
				continue
			}
			dirEnts, err := os.ReadDir(mp.config.RootDir)
			if err != nil {
				log.LogErrorf("Failed to readdir of [%s]: %v", mp.config.RootDir, err)
				continue
			}
			for _, ent := range dirEnts {
				if !ent.IsDir() {
					continue
				}
				if strings.HasPrefix(ent.Name(), CheckpointDirExpPrefix) {
					dir := path.Join(mp.config.RootDir, ent.Name())
					os.RemoveAll(dir)
					log.LogInfof("Remove [%s]", dir)
					log.LogCriticalf("DEBUG: Part(%v) Remove [%s]", mp.config.RootDir, dir)
					// FIXME: sleep a while?
					time.Sleep(time.Second)
				}
			}
			// FIXME: use a new channel to receive expired directory directly?
		case <-mp.metaDB.stopC:
			return
		}
	}
}

func (mp *metaPartition) removeOldestInodeLRU() {
	var (
		reclaimed int
		scanned   int
		now       time.Time
		nowUnix   int64
	)

	if mp.metaDB.TestStatus(MetaDBNew) {
		log.LogCriticalf("DEBUG: part[%v] is a new db, wait snapshot", mp.config.PartitionId)
		// this is a new DB, wait snapshot to save all nodes
		return
	}

	now = time.Now()
	nowUnix = now.Unix()
	mp.inodeTree.Lock()
	log.LogCriticalf("DEBUG: start reclaim part[%v] lru len[%v]", mp.config.PartitionId, mp.inodeTree.lru.Len())
	defer func() {
		log.LogCriticalf("DEBUG: end reclaim part[%v] lru len[%v] scanned[%v] reclaimed[%v]", mp.config.PartitionId, mp.inodeTree.lru.Len(), scanned, reclaimed)
	}()
	// FIXME: need a better loop to reclaim cold inodes
	// FIXME: is list iterator safe?
	// FIXME: what if the reclaimed inode is not the first one?
	item := mp.inodeTree.lru.Front()
	for mp.inodeTree.lru.Len() > ReclaimStartCnt && reclaimed < ReclaimLimit && scanned < ReclaimScanLimit {
		scanned++

		if item == nil {
			break
		}

		ino := item.Value.(*Inode)
		log.LogCriticalf("DEBUG: part[%v] try to reclaim ino[%v]", mp.config.PartitionId, ino.Inode)
		ino.Lock() // FIXME: should use trylock and skip busy ones
		// FIXME: need fix overflow?
		if now.Sub(ino.elapse) < ColdTimeMax {
			log.LogCriticalf("DEBUG: part[%v] ino[%v] elapse[%s] now[%s] diff[%s]", mp.config.PartitionId, ino.Inode,
				ino.elapse.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"), now.Sub(ino.elapse))
			// no inode is satisfied
			ino.Unlock()
			break
		}

		// FIXME: need fix overflow?
		if nowUnix-ino.AccessTime < int64(AccessTimeMax) {
			// skip inode that is accessed recently
			log.LogCriticalf("DEBUG: part[%v] ino[%v] atime[%d] now[%d] diff[%d]", mp.config.PartitionId, ino.Inode, ino.AccessTime, nowUnix, nowUnix-ino.AccessTime)
			ino.Unlock()
			item = item.Next()
			continue
		}

		if ino.refcnt > 0 {
			// referenced by someone
			log.LogCriticalf("DEBUG: part[%v] ino[%v] refcnt[%v]", mp.config.PartitionId, ino.Inode, ino.refcnt)
			ino.Unlock()
			item = item.Next()
			continue
		}

		if ino.NLink == 0 /*ino.Flag&DeleteMarkFlag == DeleteMarkFlag*/ {
			// skip orphan inode
			log.LogCriticalf("DEBUG: part[%v] ino[%v] NLink[%v] DeleteMarkFlag[%v], skip orphan", mp.config.PartitionId, ino.Inode, ino.NLink, ino.Flag&DeleteMarkFlag)
			ino.Unlock()
			item = item.Next()
			continue
		}

		if ino.IsDirty() {
			// wait storeToDB to write all diry inodes back
			ino.Unlock()
			item = item.Next()
			continue
		}
		// found the inode
		if item := mp.inodeTree.tree.Delete(ino); item != nil {
			item.(BtreeItem).DeleteLRU(mp.inodeTree.lru)
		}
		log.LogCriticalf("DEBUG: Part(%v) reclaim remove ino(%v) from memory", mp.config.PartitionId, ino.Inode)
		ino.Unlock()
		mp.inodeTree.Unlock()
		reclaimed++

		runtime.Gosched()
		mp.inodeTree.Lock()
		item = mp.inodeTree.lru.Front()
	}
	mp.inodeTree.Unlock()
}

func (mp *metaPartition) ReclaimColdInodes() {
	ticker := time.NewTicker(ReclaimInterval)

	for {
		select {
		case <-ticker.C:
			if !mp.metaDB.TestStatus(MetaDBReady) {
				continue
			}
			// do reclaim
			mp.removeOldestInodeLRU()
		case <-mp.metaDB.stopC:
			ticker.Stop()
			return
		}
	}
}

func (mp *metaPartition) ReadInodeFromDB(ino uint64, ro bool) (btree.Item, error) {
	cf, found := mp.metaDB.cfs["inode"]
	if !found {
		err := errors.New("inode Column Family not found")
		log.LogError(err)
		return nil, err
	}

	log.LogCriticalf("DEBUG: [ReadInodeFromDB] Part(%v) ino(%v)", mp.config.PartitionId, ino)
	key := []byte(fmt.Sprintf("%v", ino))
	item, err := mp.metaDB.db.GetCF(cf, key)
	if err != nil {
		// TODO: if is ENOENT, do not print error message
		log.LogErrorf("Failed to get ino %v from DB: %v", ino, err)
		return nil, err
	}

	data := item.([]byte)
	inode := &Inode{Inode:ino}
	if err = inode.Unmarshal(data); err != nil {
		log.LogErrorf("Failed to unmarshal ino %v: %v", ino, err)
		return nil, err
	}

	if status := mp.fsmCreateInode(inode); status != proto.OpOk {
		var newItem btree.Item
		// someone loads the same inode for us
		if ro {
			newItem = mp.inodeTree.GetForRead(inode)
		} else {
			newItem = mp.inodeTree.GetForWrite(inode)
		}
		if newItem == nil {
			err = fmt.Errorf("Create new inode ino(%v) get %v but still cannot get inode from tree",
				ino, status)
			log.LogError(err)
			return nil, err
		}
		inode = newItem.(*Inode)
	}
	return inode, nil
}
