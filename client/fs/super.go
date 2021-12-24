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

package fs

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
)

// Super defines the struct of a super block.
type Super struct {
	cluster     string
	volname     string
	owner       string
	ic          *InodeCache
	mw          *meta.MetaWrapper
	ec          *stream.ExtentClient
	orphan      *OrphanInodeList
	enSyncWrite bool
	keepCache   bool

	nodeCache map[uint64]fs.Node
	fslock    sync.Mutex

	disableDcache bool
	fsyncOnClose  bool
	enableXattr   bool
	rootIno       uint64

	state     fs.FSStatType
	sockaddr  string
	suspendCh chan interface{}
}

// Functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

// NewSuper returns a new Super.
func NewSuper(opt *proto.MountOptions) (s *Super, err error) {
	s = new(Super)
	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	var metaConfig = &meta.MetaConfig{
		Volume:        opt.Volname,
		Owner:         opt.Owner,
		AccessKey:     opt.AccessKey,
		SecretKey:     opt.SecretKey,
		Masters:       masters,
		Authenticate:  opt.Authenticate,
		TicketMess:    opt.TicketMess,
		ValidateOwner: opt.Authenticate || opt.AccessKey == "",
	}
	s.mw, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewMetaWrapper failed!")
	}

	s.volname = opt.Volname
	s.owner = opt.Owner
	s.cluster = s.mw.Cluster()
	inodeExpiration := DefaultInodeExpiration
	if opt.IcacheTimeout >= 0 {
		inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
	}
	if opt.LookupValid >= 0 {
		LookupValidDuration = time.Duration(opt.LookupValid) * time.Second
	}
	if opt.AttrValid >= 0 {
		AttrValidDuration = time.Duration(opt.AttrValid) * time.Second
	}
	if opt.EnSyncWrite > 0 {
		s.enSyncWrite = true
	}
	s.keepCache = opt.KeepCache
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	s.nodeCache = make(map[uint64]fs.Node)
	s.disableDcache = opt.DisableDcache
	s.fsyncOnClose = opt.FsyncOnClose
	s.enableXattr = opt.EnableXattr

	var extentConfig = &stream.ExtentConfig{
		Volume:            opt.Volname,
		Masters:           masters,
		FollowerRead:      opt.FollowerRead,
		NearRead:          opt.NearRead,
		ReadRate:          opt.ReadRate,
		WriteRate:         opt.WriteRate,
		OnAppendExtentKey: s.mw.AppendExtentKey,
		OnGetExtents:      s.mw.GetExtents,
		OnTruncate:        s.mw.Truncate,
		OnEvictIcache:     s.ic.Delete,
	}
	s.ec, err = stream.NewExtentClient(extentConfig)
	if err != nil {
		return nil, errors.Trace(err, "NewExtentClient failed!")
	}

	if s.rootIno, err = s.mw.GetRootIno(opt.SubDir); err != nil {
		return nil, err
	}
	s.suspendCh = make(chan interface{})

	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

// Root returns the root directory where it resides.
func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(s.rootIno)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

// Statfs handles the Statfs request and returns a set of statistics.
func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	const defaultMaxMetaPartitionInodeID uint64 = 1<<63 - 1
	total, used, inodeCount := s.mw.Statfs()
	resp.Blocks = total / uint64(DefaultBlksize)
	resp.Bfree = (total - used) / uint64(DefaultBlksize)
	resp.Bavail = resp.Bfree
	resp.Bsize = DefaultBlksize
	resp.Namelen = DefaultMaxNameLen
	resp.Frsize = DefaultBlksize
	resp.Files = inodeCount
	resp.Ffree = defaultMaxMetaPartitionInodeID - inodeCount
	return nil
}

// ClusterName returns the cluster name.
func (s *Super) ClusterName() string {
	return s.cluster
}

func (s *Super) GetRate(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(s.ec.GetRate()))
}

func (s *Super) SetRate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if rate := r.FormValue("read"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set read rate failed\n"))
		} else {
			msg := s.ec.SetReadRate(val)
			w.Write([]byte(fmt.Sprintf("Set read rate to %v successfully\n", msg)))
		}
	}

	if rate := r.FormValue("write"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set write rate failed\n"))
		} else {
			msg := s.ec.SetWriteRate(val)
			w.Write([]byte(fmt.Sprintf("Set write rate to %v successfully\n", msg)))
		}
	}
}

func (s *Super) exporterKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%v_fuseclient_%v", s.cluster, act)
}

func (s *Super) handleError(op, msg string) {
	log.LogError(msg)
	ump.Alarm(s.umpKey(op), msg)
}

func replyFail(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(msg))
}

func replySucc(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(msg))
}

func (s *Super) SetSuspend(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ret string
	)

	if err = r.ParseForm(); err != nil {
		replyFail(w, r, err.Error())
		return
	}

	sockaddr := r.FormValue("sock")
	if sockaddr == "" {
		err = fmt.Errorf("Need parameter 'sock' for IPC")
		replyFail(w, r, err.Error())
		return
	}

	s.fslock.Lock()
	if s.sockaddr != "" ||
		!atomic.CompareAndSwapUint32((*uint32)(&s.state), uint32(fs.FSStatResume), uint32(fs.FSStatSuspend)) {
		s.fslock.Unlock()
		err = fmt.Errorf("Already in suspend: sock '%s', state %v", s.sockaddr, s.state)
		replyFail(w, r, err.Error())
		return
	}
	s.sockaddr = sockaddr
	s.fslock.Unlock()

	// wait
	msg := <-s.suspendCh
	switch msg.(type) {
	case error:
		err = msg.(error)
	case string:
		ret = msg.(string)
	default:
		err = fmt.Errorf("Unknown return type: %v", msg)
	}

	if err != nil {
		s.fslock.Lock()
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
		s.sockaddr = ""
		s.fslock.Unlock()
		replyFail(w, r, err.Error())
		return
	}

	if !atomic.CompareAndSwapUint32((*uint32)(&s.state), uint32(fs.FSStatSuspend), uint32(fs.FSStatShutdown)) {
		s.fslock.Lock()
		atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
		s.sockaddr = ""
		s.fslock.Unlock()
		err = fmt.Errorf("Invalid old state %v", s.state)
		replyFail(w, r, err.Error())
		return
	}

	replySucc(w, r, fmt.Sprintf("set suspend successfully: %s", ret))
}

func (s *Super) SetResume(w http.ResponseWriter, r *http.Request) {
	atomic.StoreUint32((*uint32)(&s.state), uint32(fs.FSStatResume))
	replySucc(w, r, "set resume successfully")
}

func (s *Super) State() (state fs.FSStatType, sockaddr string) {
	return fs.FSStatType(atomic.LoadUint32((*uint32)(&s.state))), s.sockaddr
}

func (s *Super) Notify(stat fs.FSStatType, msg interface{}) {
	if stat == fs.FSStatSuspend {
		s.suspendCh <- msg
	}
}
