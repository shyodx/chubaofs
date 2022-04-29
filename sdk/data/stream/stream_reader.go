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

package stream

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gammazero/workerpool"
)

// One inode corresponds to one streamer. All the requests to the same inode will be queued.
// TODO rename streamer here is not a good name as it also handles overwrites, not just stream write.
type Streamer struct {
	client *ExtentClient
	inode  uint64

	status int32

	refcnt int

	idle      int // how long there is no new request
	traversed int // how many times the streamer is traversed

	extents *ExtentCache
	once    sync.Once

	newStreamer bool
	createTime  time.Time

	handler   *ExtentHandler   // current open handler
	dirtylist *DirtyExtentList // dirty handlers
	dirty     bool             // whether current open handler is in the dirty list

	request chan interface{} // request channel, write/flush/close
	done    chan struct{}    // stream writer is being closed

	owRequest    chan *ExtentRequest
	owStatus     int32
	owInflight   int32
	owEmpty      chan struct{}
	doneOwServer chan struct{}
	owDpStatus   map[string]*owDpExtStatus // no need to be protected by lock.

	writeLock sync.Mutex
}

type owDpExtStatus struct {
	sync.Mutex
	inflight int32
	eks      map[int]int
	empty    chan struct{}
}

func (dps *owDpExtStatus) intersect(offset, size int) bool {
	var maxLeft, minRight int

	if s, ok := dps.eks[offset]; ok {
		if s != 0 {
			return true
		} else {
			return false
		}
	}

	for o, s := range dps.eks {
		if o < offset {
			maxLeft = offset
		} else {
			maxLeft = o
		}
		if o+s < offset+size {
			minRight = o + s
		} else {
			minRight = offset + size
		}
		if maxLeft < minRight {
			return true
		}
	}
	return false
}

var owWorkerPool = workerpool.New(256)

// NewStreamer returns a new streamer.
func NewStreamer(client *ExtentClient, inode uint64) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, 64)
	s.done = make(chan struct{})
	s.dirtylist = NewDirtyExtentList()
	s.owRequest = make(chan *ExtentRequest, 128)
	s.owEmpty = make(chan struct{}, 8)
	s.doneOwServer = make(chan struct{})
	s.owDpStatus = make(map[string]*owDpExtStatus)
	go s.owServer()
	go s.server()
	return s
}

func (s *Streamer) SetNewlyCreatedStreamer() {
	// no need to set newStreamer as false explicitly
	// the ShouldGetExtents is only checked in Streamer.once.Do, so it is only
	// work once
	s.newStreamer = true
	s.createTime = time.Now()
}

func (s *Streamer) ShouldGetExtents() bool {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	if !s.newStreamer {
		return true
	}
	now := time.Now()
	if now.Sub(s.createTime) > time.Minute {
		s.newStreamer = false
		return true
	}
	return false
}

// String returns the string format of the streamer.
func (s *Streamer) String() string {
	return fmt.Sprintf("Streamer{ino(%v)}", s.inode)
}

// TODO should we call it RefreshExtents instead?
func (s *Streamer) GetExtents() error {
	return s.extents.Refresh(s.inode, s.client.getExtents)
}

// GetExtentReader returns the extent reader.
// TODO: use memory pool
func (s *Streamer) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	partition, err := s.client.dataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}
	reader := NewExtentReader(s.inode, ek, partition, s.client.dataWrapper.FollowerRead())
	return reader, nil
}

func (s *Streamer) readExtentParellel(
	wg *sync.WaitGroup,
	lastone bool,
	req *ExtentRequest,
	rsizep *int64,
	retCh chan error) {
	var (
		reader    *ExtentReader
		readBytes int
		err       error
	)

	defer func() {
		if err != nil && err != io.EOF {
			log.LogErrorf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)
		}
		wg.Done()
	}()

	if req.ExtentKey == nil {
		for i := range req.Data {
			// FIXME: need fill zero? or req.Data is already all zero?
			req.Data[i] = 0
		}

		filesize, _ := s.extents.Size()
		if req.FileOffset+req.Size > filesize {
			if req.FileOffset > filesize {
				return
			}
			req.Size = filesize - req.FileOffset
			atomic.AddInt64(rsizep, int64(req.Size))
			retCh <- io.EOF
			return
		}

		// Reading a hole, just fill zero
		atomic.AddInt64(rsizep, int64(req.Size))
		log.LogErrorf("Stream read hole: ino(%v) req(%v) total(%v)", s.inode, req, *rsizep)
	} else {
		reader, err = s.GetExtentReader(req.ExtentKey)
		if err != nil {
			retCh <- err
			return
		}
		readBytes, err = reader.Read(req)
		log.LogErrorf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)
		atomic.AddInt64(rsizep, int64(readBytes))
		if err != nil {
			retCh <- err
			return
		}
		if readBytes < req.Size {
			if !lastone {
				panic(fmt.Sprintf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err))
			}
		}
	}
	return
}

func (s *Streamer) read(data []byte, offset int, size int) (total int, err error) {
	var (
		requests        []*ExtentRequest
		revisedRequests []*ExtentRequest
		wg              sync.WaitGroup
		rsize           int64 //atomic value
	)

	ctx := context.Background()
	s.client.readLimiter.Wait(ctx)

	requests = s.extents.PrepareReadRequests(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		if req.ExtentKey.PartitionId == 0 || req.ExtentKey.ExtentId == 0 {
			s.writeLock.Lock()
			if err = s.IssueFlushRequest(); err != nil {
				s.writeLock.Unlock()
				return 0, err
			}
			revisedRequests = s.extents.PrepareReadRequests(offset, size, data)
			s.writeLock.Unlock()
			break
		}
	}

	if revisedRequests != nil {
		requests = revisedRequests
	}

	filesize, _ := s.extents.Size()
	log.LogDebugf("read: ino(%v) requests(%v) filesize(%v)", s.inode, requests, filesize)
	log.LogErrorf("read: ino(%v) offs(%v) len(%v) requests(%v) filesize(%v)", s.inode, offset, size, len(requests), filesize)
	parellelLevel := util.Min(len(requests), 10)
	retCh := make(chan error, parellelLevel)
	for i, req := range requests {
		if i == parellelLevel {
			wg.Wait()
		}
		if len(retCh) != 0 {
			err = <-retCh
			if err != io.EOF {
				break
			}
		}
		wg.Add(1)
		go s.readExtentParellel(&wg, i == len(requests), req, &rsize, retCh)
	}
	wg.Wait()

	total = int(rsize)
	log.LogErrorf("read: ino(%v) offs(%v) len(%v) requests(%v) filesize(%v) rsize(%v)", s.inode, offset, size, len(requests), filesize, total)
	return
}
