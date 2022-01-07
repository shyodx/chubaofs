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

package wrapper

import (
	"container/list"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	ExtentFileChunkNr               = 10
	ExtentFileChunkCongestWaterMark = ExtentFileChunkNr + ExtentFileChunkNr / 3
	ExtentFileChunkSize             = util.BlockSize // XXX: must be power of 2
	ExtentFileContentExpireDuration = 10 * time.Minute
)

type ExtentFileStat struct {
	PartID     uint64
	Count      int
	Create     uint64
	Overwrite  uint64
	Hit        uint64
	Miss       uint64
	Tiny       uint64
	Expired    uint64
	Outofrange uint64
}

type ExtentFileLRU struct {
	sync.Mutex
	partitionID uint64
	head        *list.List

	// statistics
	create     uint64
	overwrite  uint64
	hit        uint64
	miss       uint64
	tiny       uint64
	expired    uint64
	outofrange uint64
}

// Read only after init
type ExtentFileChunk struct {
	PartitionID uint64
	ExtentID    uint64
	UpdateTime  time.Time

	Offset int
	Size   int
	Data   []byte
}

// DataPartition defines the wrapper of the data partition.
type DataPartition struct {
	// Will not be changed
	proto.DataPartitionResponse
	RandomWrite   bool
	PartitionType string
	NearHosts     []string
	ClientWrapper *Wrapper
	Metrics       *DataPartitionMetrics

	ExtFileLRU *ExtentFileLRU
}

func (dp *DataPartition) InvalidateCache(extID uint64) {
	dp.ExtFileLRU.Remove(extID)
}

func NewExtentFileLRU(partitionID uint64) *ExtentFileLRU {
	return &ExtentFileLRU{
		partitionID: partitionID,
		head:        list.New(),
	}
}

func NewExtentFileChunk(partID, extID uint64) *ExtentFileChunk {
	return &ExtentFileChunk{
		PartitionID: partID,
		ExtentID:    extID,
	}
}

func (efc *ExtentFileChunk) SetData(data []byte, offs int, size int) {
	efc.Data = data
	efc.Offset = offs
	efc.Size = size
	efc.UpdateTime = time.Now()
}

func (efc *ExtentFileChunk) TryReadData(data []byte, offs int, size int) int {
	var start int = offs - efc.Offset

	rsize := copy(data, efc.Data[start:])
	if rsize != size {
		log.LogErrorf("Not enough data read: chunk(part:%v ext:%v offs:%v size:%v) req(offs:%v size:%v) read:%v",
			efc.PartitionID, efc.ExtentID, efc.Offset, efc.Size, offs, size, rsize)

		rsize = 0
	}

	return rsize
}

func (lru *ExtentFileLRU) insertTailNoLock(efc *ExtentFileChunk) {
	lru.head.PushBack(efc)
	if lru.head.Len() >= ExtentFileChunkNr {
		first := lru.head.Front()
		efc := first.Value.(*ExtentFileChunk)
		if time.Now().After(efc.UpdateTime.Add(30 * time.Second)) {
			lru.head.Remove(first)
		}
		log.LogErrorf("lru too long %v", lru.head.Len())
	}
}

func (lru *ExtentFileLRU) lookupNoLock(extID uint64, offset int, size int, create bool) *ExtentFileChunk {
	var efc *ExtentFileChunk
	var found bool

	for item := lru.head.Front(); item != nil; item = item.Next() {
		efc = item.Value.(*ExtentFileChunk)
		if efc.ExtentID != extID {
			log.LogErrorf("lookupNoLock: skip part %v ext %v", efc.PartitionID, efc.ExtentID)
			continue
		}

		found = true
		if create {
			// create will overwrite this chunk
			lru.head.Remove(item)
			break
		}

		log.LogErrorf("lookupNoLock: found ExtentFileChunk(part:%v ext:%v offs:%v size:%v) req(offs:%v size:%v)",
			efc.PartitionID, efc.ExtentID, efc.Offset, efc.Size, offset, size)
		if efc.UpdateTime.After(time.Now().Add(ExtentFileContentExpireDuration)) {
			// expired
			log.LogErrorf("Expired update time %v", efc.UpdateTime)
			lru.IncCount(LRUExpired)
			lru.head.Remove(item)
			efc = nil
			break
		}

		if offset < efc.Offset || offset+size > efc.Offset+efc.Size {
			// out of range
			lru.IncCount(LRUOutOfRange)
			lru.head.Remove(item)
			efc = nil
			break
		}

		lru.IncCount(LRUHit)
		lru.head.MoveToBack(item)
		break
	}

	if !create && !found {
		lru.IncCount(LRUMiss)
	}

	return efc
}

func (lru *ExtentFileLRU) Congested() bool {
	size := lru.head.Len()
	if size >= ExtentFileChunkCongestWaterMark {
		log.LogErrorf("lru too long %v", size)
		return true
	}
	return false
}

func (lru *ExtentFileLRU) Lookup(extID uint64, offset int, size int) *ExtentFileChunk {
	var efc *ExtentFileChunk

	lru.Lock()
	efc = lru.lookupNoLock(extID, offset, size, false)
	lru.Unlock()

	return efc
}

func (lru *ExtentFileLRU) Create(extID uint64, offset int, size int, data []byte) *ExtentFileChunk {
	var efc *ExtentFileChunk

	log.LogErrorf("Create Chunk: partID %v extID %v offs %v size %v", lru.partitionID, extID, offset, size)
	lru.Lock()
	efc = lru.lookupNoLock(extID, offset, size, true)
	if efc == nil {
		efc = NewExtentFileChunk(lru.partitionID, extID)
		lru.IncCount(LRUCreate)
	} else {
		lru.IncCount(LRUOverwrite)
	}
	efc.SetData(data, offset, size)
	lru.insertTailNoLock(efc)
	lru.Unlock()

	return efc
}

type LRUCountType int

const (
	LRUTiny LRUCountType = iota
	LRUHit
	LRUMiss
	LRUExpired
	LRUOutOfRange
	LRUCreate
	LRUOverwrite
)

func (lru *ExtentFileLRU) IncCount(t LRUCountType) {
	switch t {
	case LRUTiny:
		atomic.AddUint64(&lru.tiny, 1)
	case LRUHit:
		atomic.AddUint64(&lru.hit, 1)
	case LRUMiss:
		atomic.AddUint64(&lru.miss, 1)
	case LRUExpired:
		atomic.AddUint64(&lru.expired, 1)
	case LRUOutOfRange:
		atomic.AddUint64(&lru.outofrange, 1)
	case LRUCreate:
		atomic.AddUint64(&lru.create, 1)
	case LRUOverwrite:
		atomic.AddUint64(&lru.overwrite, 1)
	}
}

func (lru *ExtentFileLRU) Remove(extID uint64) bool {
	var found bool

	lru.Lock()
	for item := lru.head.Front(); item != nil; item = item.Next() {
		efc := item.Value.(*ExtentFileChunk)
		if efc.ExtentID == extID {
			lru.head.Remove(item)
			found = true
			break
		}
	}
	lru.Unlock()

	return found
}

// DataPartitionMetrics defines the wrapper of the metrics related to the data partition.
type DataPartitionMetrics struct {
	sync.RWMutex
	AvgReadLatencyNano  int64
	AvgWriteLatencyNano int64
	SumReadLatencyNano  int64
	SumWriteLatencyNano int64
	ReadOpNum           int64
	WriteOpNum          int64
}

func (dp *DataPartition) RecordWrite(startT int64) {
	if startT == 0 {
		log.LogWarnf("RecordWrite: invalid start time")
		return
	}
	cost := time.Now().UnixNano() - startT

	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	dp.Metrics.WriteOpNum++
	dp.Metrics.SumWriteLatencyNano += cost

	return
}

func (dp *DataPartition) MetricsRefresh() {
	dp.Metrics.Lock()
	defer dp.Metrics.Unlock()

	if dp.Metrics.ReadOpNum != 0 {
		dp.Metrics.AvgReadLatencyNano = dp.Metrics.SumReadLatencyNano / dp.Metrics.ReadOpNum
	} else {
		dp.Metrics.AvgReadLatencyNano = 0
	}

	if dp.Metrics.WriteOpNum != 0 {
		dp.Metrics.AvgWriteLatencyNano = dp.Metrics.SumWriteLatencyNano / dp.Metrics.WriteOpNum
	} else {
		dp.Metrics.AvgWriteLatencyNano = 0
	}

	dp.Metrics.SumReadLatencyNano = 0
	dp.Metrics.SumWriteLatencyNano = 0
	dp.Metrics.ReadOpNum = 0
	dp.Metrics.WriteOpNum = 0
}

func (dp *DataPartition) GetAvgRead() int64 {
	dp.Metrics.RLock()
	defer dp.Metrics.RUnlock()

	return dp.Metrics.AvgReadLatencyNano
}

func (dp *DataPartition) GetAvgWrite() int64 {
	dp.Metrics.RLock()
	defer dp.Metrics.RUnlock()

	return dp.Metrics.AvgWriteLatencyNano
}

type DataPartitionSorter []*DataPartition

func (ds DataPartitionSorter) Len() int {
	return len(ds)
}
func (ds DataPartitionSorter) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}
func (ds DataPartitionSorter) Less(i, j int) bool {
	return ds[i].Metrics.AvgWriteLatencyNano < ds[j].Metrics.AvgWriteLatencyNano
}

// NewDataPartitionMetrics returns a new DataPartitionMetrics instance.
func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	return metrics
}

// String returns the string format of the data partition.
func (dp *DataPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v) NearHosts(%v)",
		dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts, dp.NearHosts)
}

func (dp *DataPartition) CheckAllHostsIsAvail(exclude map[string]struct{}) {
	var (
		conn net.Conn
		err  error
	)
	for i := 0; i < len(dp.Hosts); i++ {
		host := dp.Hosts[i]
		if conn, err = util.DailTimeOut(host, proto.ReadDeadlineTime*time.Second); err != nil {
			log.LogWarnf("Dail to Host (%v) err(%v)", host, err.Error())
			if strings.Contains(err.Error(), syscall.ECONNREFUSED.Error()) {
				exclude[host] = struct{}{}
			}
			continue
		}
		conn.Close()
	}

}

// GetAllAddrs returns the addresses of all the replicas of the data partition.
func (dp *DataPartition) GetAllAddrs() string {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

func isExcluded(dp *DataPartition, exclude map[string]struct{}) bool {
	for _, host := range dp.Hosts {
		if _, exist := exclude[host]; exist {
			return true
		}
	}
	return false
}
