// Copyright 2022 The ChubaoFS Authors.
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

package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"syscall"

	"github.com/chubaofs/chubaofs/util/log"
)

type CmdType int

const (
	CmdRegisterNew CmdType = iota + 1
	CmdRegisterClone
	CmdUnregister
	CmdWakeup
)

type QueueType int

const (
	CtrlQueue QueueType = iota
	DataQueue
	DoneQueue
	MaxQueue
)

const (
	// ctrl related
	CtrlItemSize = 64
	CtrlDataSize = 40

	CtrlStateNew        = 0
	CtrlStateReady      = 0x00000001
	CtrlStateDataItem   = 0x10000000
	CtrlStateInlineData = 0x20000000

	// done related
	DoneItemSize = 24

	DoneStateNew   = 0
	DoneStateReady = 1
	DoneStateStale = 2

	// data related
	DataItemSize = 4096
	DataDataSize = 4080 //4076

	DataSlotEnd uint32 = ^uint32(0)
)

type CtrlItem struct {
	ReqId   uint64             // unique id for request
	OpCode  uint32             //
	State   uint32             // atomic: DATA_ITEM | INLINE_DATA
	DoneIdx uint32             // where to put return value
	DataIdx uint32             // start from this slot in data queue
	data    [CtrlDataSize]byte // for short params and da
}

type DoneItem struct {
	ReqId  uint64 // the same with ctrl_item
	OpCode uint32
	State  uint32 // atomic: FRESH or STALE
	RetVal int64
}

// DataItem is a chain, Next indicates the position of the next item
type DataItem struct {
	ReqId  uint64 // the same with ctrl_item
	OpCode uint32
	Next   uint32 // next data item, 0xffffffff means end of chain
	data   [DataDataSize]byte
}

func (item *CtrlItem) String() string {
	str := fmt.Sprintf("ReqId:%v OpCode:%v State:%x DoneIdx:%v DataIdx:%v",
		item.ReqId, item.OpCode, item.State, item.DoneIdx, item.DataIdx)
	if item.State&CtrlStateInlineData == CtrlStateInlineData {
		str = fmt.Sprintf("CtrlItem(%s Data:%s)", str, string(item.data[:]))
	} else {
		str = fmt.Sprintf("CtrlItem(%s Data:<in data item>)", str)
	}
	return str
}

func (item *DoneItem) String() string {
	return fmt.Sprintf("DoneItem(ReqId:%v OpCode:%v State:%x RetVal:%v)",
		item.ReqId, item.OpCode, item.State, item.RetVal)
}

func (item *DataItem) String() string {
	return fmt.Sprintf("DataItem(ReqId:%v OpCode:%v Next:%x",
		item.ReqId, item.OpCode, item.Next)
}

const QueueSuspendFlag uint32 = 0x1

type QueueHeader struct {
	head     uint32
	tail     uint32
	flags    uint32
	reserved uint32
}

type QueueHeaderPtr struct {
	head  *uint32
	tail  *uint32
	flags *uint32
}

// An QueueInfo is based on share memory object. It behaves like a ring buffer
// of a bunch of slots. Each slot has an equal size.
type QueueInfo struct {
	queueType QueueType
	// shmFile is a shared memory object opened by shm_open, the object
	// path is registered by app
	//shmFile *os.File
	// size is the size of a slot
	size uint16
	// members is the number of slots
	members uint32
	mask    uint32
	// head is the index of current write position
	head uint32
	// tail is the index of current read position
	tail uint32
	// filename is the name of shmFile
	namelen  uint16
	filename []byte

	// data is base address of shared memory mapped by shared memory object
	addr []byte
	hdr  QueueHeaderPtr
	data []byte
}

// An QueueArray represents a direct connection between app and FUSE daemon
type QueueArray struct {
	// ctrl is a read-only queue which saves command request from app
	ctrl *QueueInfo
	// data is a read-only queue which saves request data from app, it is
	// indexed by command request
	data *QueueInfo
	// done is a write-only queue which saves done command request
	done *QueueInfo
}

func parseQueueInfo(data []byte) (*QueueInfo, int) {
	var start, end int

	queue := &QueueInfo{}

	/* FIXME: need check validation of input params */
	end = 2
	queue.namelen = binary.BigEndian.Uint16(data[start:end])
	start = end
	end += 2
	queue.size = binary.BigEndian.Uint16(data[start:end])
	start = end
	end += 4
	queue.members = binary.BigEndian.Uint32(data[start:end])
	queue.mask = queue.members - 1
	//log.LogDebugf("namelen %v size %v members %v", queue.namelen, queue.size, queue.members)
	fmt.Printf("namelen %v size %v members %v end %v\n", queue.namelen, queue.size, queue.members, end+int(queue.namelen))
	queue.filename = make([]byte, queue.namelen)
	start = end
	end += int(queue.namelen)
	copy(queue.filename, data[start:end])

	return queue, end
}

func handleRegisterNew(data []byte) (int, []byte) {
	var (
		idStr []byte = make([]byte, 8) // 8 is enough for a 64bit value
		offs  int
	)

	return offs, idStr
}

func handleRegisterClone(data []byte) (int, []byte) {
	var (
		idStr []byte = make([]byte, 8) // 8 is enough for a 64bit value
		offs  int
	)

	return offs, idStr
}

func handleUnregister(data []byte) (int, []byte) {
	var (
		ret  []byte = make([]byte, 8) // 8 is enough for a 64bit value
		offs int
	)

	return offs, ret
}

func handleWakeup(data []byte) (int, []byte) {
	var (
		ret  []byte = make([]byte, 8) // 8 is enough for a 64bit value
		offs int
	)

	return offs, ret
}

func HandleCmd(wg *sync.WaitGroup, conn net.Conn) {
	var (
		retData []byte
		rsize   int
		wsize   int
		offs    int
		n       int
		err     error
	)

	defer func() {
		conn.Close()
		wg.Done()
	}()

	data := make([]byte, 4096) // FIXME: fix the size
	if rsize, err = conn.Read(data); err != nil {
		log.LogErrorf("Failed to read conn: %v", err)
		return
	}
	//fmt.Printf("DEBUG: read data size %v (%v)\n", rsize, data[0:rsize])
	fmt.Printf("DEBUG: read data size %v\n", rsize)

	// cmd format:
	//   type uint16
	//   data []byte
	for offs = 0; offs < rsize; {
		cmd := CmdType(binary.BigEndian.Uint16(data[offs : offs+2]))
		fmt.Printf("DEBUG: get cmd %v offs %v\n", cmd, offs)
		offs += 2
		switch cmd {
		case CmdRegisterNew:
			n, retData = handleRegisterNew(data[offs:])

		case CmdRegisterClone:
			n, retData = handleRegisterClone(data[offs:])

		case CmdUnregister:
			n, retData = handleUnregister(data[offs:])

		case CmdWakeup:
			n, retData = handleWakeup(data[offs:])

		default:
			log.LogErrorf("Invalid cmd %v", cmd)
			retData = make([]byte, 8)
			errno := Errno(syscall.EINVAL)
			binary.BigEndian.PutUint64(retData, uint64(errno))
		}

		offs += n
	}

	fmt.Printf("DEBUG: return value to app offs %v rsize %v\n", offs, rsize)
	if wsize, err = conn.Write(retData); err != nil {
		log.LogErrorf("Failed to write conn: %v", err)
	}

	fmt.Printf("DEBUG: write %v bytes %v\n", wsize, retData)
}
