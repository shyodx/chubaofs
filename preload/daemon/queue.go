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
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/chubaofs/chubaofs/libsdk/comm"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	PAGESIZE uint32 = 4096
)

// align must be power of 2
func RoundUp(size uint32, align uint32) uint32 {
	return (size + (align - 1)) / align * align
}

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

func ShmOpen(filename string, flag int, perm os.FileMode) (*os.File, error) {
	var shmFlags int = syscall.O_NOFOLLOW | syscall.O_CLOEXEC | syscall.O_NONBLOCK
	// FIXME: should parse and check filename like c library
	filePath := fmt.Sprintf("/dev/shm/%s", filename)
	return os.OpenFile(filePath, flag|shmFlags, perm)
}

func ShmUnlink(filename string) error {
	// FIXME: should parse and check filename like c library
	filePath := fmt.Sprintf("/dev/shm/%s", filename)
	return os.Remove(filePath)
}

func ShmClose(file *os.File) {
	file.Close()
}

func mapQueue(queue *QueueInfo) (err error) {
	var shmFile *os.File

	if shmFile, err = ShmOpen(string(queue.filename), os.O_RDWR, 0666); err != nil {
		log.LogErrorf("failed to open %v: %v", string(queue.filename), err)
		return
	}
	defer ShmClose(shmFile)

	length := int(RoundUp(uint32(queue.size)*queue.members, PAGESIZE))
	queue.addr, err = syscall.Mmap(int(shmFile.Fd()), 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		log.LogErrorf("failed to mmap %v: %v", string(queue.filename), err)
		return
	}

	switch queue.queueType {
	case CtrlQueue:
		sliceHdr1 := (*reflect.SliceHeader)(unsafe.Pointer(&queue.addr))
		sliceHdr2 := (*reflect.SliceHeader)(unsafe.Pointer(&queue.data))
		sliceHdr2.Data = sliceHdr1.Data + uintptr(unsafe.Sizeof(QueueHeader{}))
		sliceHdr2.Len = sliceHdr1.Len - int(unsafe.Sizeof(QueueHeader{}))
		sliceHdr2.Cap = sliceHdr1.Cap - int(unsafe.Sizeof(QueueHeader{}))
		queue.hdr.head = (*uint32)(unsafe.Pointer(sliceHdr1.Data))
		queue.hdr.tail = (*uint32)(unsafe.Pointer(sliceHdr1.Data + unsafe.Sizeof(uint32(0))))
		queue.hdr.flags = (*uint32)(unsafe.Pointer(sliceHdr1.Data + unsafe.Sizeof(uint32(0))*2))
		fmt.Printf("DEBUG: queue %v addr %p data %v head %p tail %p flags %p\n",
			queue.queueType, unsafe.Pointer(sliceHdr1.Data), unsafe.Pointer(sliceHdr2.Data),
			queue.hdr.head, queue.hdr.tail, queue.hdr.flags)
	case DataQueue, DoneQueue:
		sliceHdr1 := (*reflect.SliceHeader)(unsafe.Pointer(&queue.addr))
		sliceHdr2 := (*reflect.SliceHeader)(unsafe.Pointer(&queue.data))
		sliceHdr2.Data = sliceHdr1.Data
		sliceHdr2.Len = sliceHdr1.Len
		sliceHdr2.Cap = sliceHdr1.Cap
		fmt.Printf("DEBUG: queue %v addr %p data %p\n",
			queue.queueType, unsafe.Pointer(sliceHdr1.Data), unsafe.Pointer(sliceHdr2.Data))
	}

	return nil
}

func (queue *QueueInfo) QueueItem(idx uint32) interface{} {
	sliceHdr := (*reflect.SliceHeader)(unsafe.Pointer(&queue.data))

	if idx > queue.members {
		return nil
	}

	switch queue.queueType {
	case CtrlQueue:
		return (*CtrlItem)(unsafe.Pointer(sliceHdr.Data + uintptr(CtrlItemSize*idx)))
	case DataQueue:
		return (*DataItem)(unsafe.Pointer(sliceHdr.Data + uintptr(DataItemSize*idx)))
	case DoneQueue:
		return (*DoneItem)(unsafe.Pointer(sliceHdr.Data + uintptr(DoneItemSize*idx)))
	}

	return syscall.EINVAL
}

func (item *DoneItem) QueueMarkItemDone(retVal int64) {
	item.RetVal = retVal
	/* FIXME: need write barrier??? */
	atomic.StoreUint32(&item.State, DoneStateReady)
}

func handleRegisterNew(data []byte) (int, []byte) {
	var (
		idStr        []byte = make([]byte, 8) // 8 is enough for a 64bit value
		offs         int
		length       int
		initQueueNum QueueType
		queueArray   *QueueArray = &QueueArray{}
		err          error
	)

	// cmd registernew format:
	//   rsvd1     uint16
	//   rsvd2     uint32
	//   rsvd3     uint64
	//   queueinfo []
	offs += 14

	for offs < len(data) {
		if initQueueNum == MaxQueue {
			break
		}

		queueType := QueueType(binary.BigEndian.Uint16(data[offs : offs+2]))
		fmt.Printf("DEBUG: get type %v\n", queueType)
		offs += 2
		switch queueType {
		case CtrlQueue:
			queueArray.ctrl, length = parseQueueInfo(data[offs:])
			queueArray.ctrl.queueType = queueType
			offs += length
			fmt.Printf("DEBUG: offs %v ctrl %s\n", offs, string(queueArray.ctrl.filename))
			if err = mapQueue(queueArray.ctrl); err != nil {
				errno := Errno(syscall.ENODEV)
				binary.BigEndian.PutUint64(idStr, uint64(errno))
				return offs, idStr
			}
		case DataQueue:
			queueArray.data, length = parseQueueInfo(data[offs:])
			queueArray.data.queueType = queueType
			offs += length
			fmt.Printf("DEBUG: offs %v data %s\n", offs, string(queueArray.data.filename))
			if err = mapQueue(queueArray.data); err != nil {
				errno := Errno(syscall.ENODEV)
				binary.BigEndian.PutUint64(idStr, uint64(errno))
				return offs, idStr
			}
		case DoneQueue:
			queueArray.done, length = parseQueueInfo(data[offs:])
			queueArray.done.queueType = queueType
			offs += length
			fmt.Printf("DEBUG: offs %v done %s\n", offs, string(queueArray.done.filename))
			if err = mapQueue(queueArray.done); err != nil {
				errno := Errno(syscall.ENODEV)
				binary.BigEndian.PutUint64(idStr, uint64(errno))
				return offs, idStr
			}
		default:
			err = errors.NewErrorf("invalid queue type %v", queueType)
			fmt.Printf("invalid queue type %v\n", queueType)
			errno := Errno(syscall.EINVAL)
			binary.BigEndian.PutUint64(idStr, uint64(errno))
			return offs, idStr
		}

		initQueueNum++
	}

	cid := comm.CFSNewClient()

	task := newTask(queueArray)
	var tid int64
	if tid, err = comm.CFSRegisterTask(cid, task); err != nil {
		panic(err.Error())
	}
	//log.LogDebugf("register request clientid[%v] taskid[%v]", cid, tid)
	fmt.Printf("DEBUG: register request Client Id %v Task Id %v\n", cid, tid)

	/* FIXME: need check return value? */
	comm.CFSSetClient(cid, "volName", "ltptest")
	comm.CFSSetClient(cid, "masterAddr", "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010")
	comm.CFSSetClient(cid, "followerRead", "true")
	comm.CFSSetClient(cid, "user", "ltptest")
	comm.CFSSetClient(cid, "ak", "39bEF4RrAQgMj6RV")
	comm.CFSSetClient(cid, "sk", "TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd")

	fmt.Printf("DEBUG: start client\n")
	if ret := comm.CFSStartClient(cid); ret < 0 {
		fmt.Printf("Failed to start client: %v\n", ret)
		binary.BigEndian.PutUint64(idStr, uint64(ret))
		return offs, idStr
	}

	// response client
	appid, app := daemon.App(cid, tid)
	binary.BigEndian.PutUint64(idStr, appid)
	fmt.Printf("DEBUG: return client id %v to app\n", appid)
	daemon.applock.Lock()
	daemon.apps[appid] = app
	daemon.applock.Unlock()

	go task.serve(daemon.ctx)

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
		ret             []byte = make([]byte, 8) // 8 is enough for a 64bit value
		offs            int
		exist           bool
		needCloseClient bool
	)

	// cmd unregister format:
	//   rsvd1 uint16
	//   rsvd2 uint32
	//   AppID uint64
	offs += 6
	appid := binary.BigEndian.Uint64(data[offs : offs+8])
	offs += 8
	daemon.applock.RLock()
	// FIXME: need increase refcnt of app?
	app, exist := daemon.apps[appid]
	delete(daemon.apps, appid)
	if len(daemon.apps) == 0 {
		needCloseClient = true
	}
	daemon.applock.RUnlock()
	if !exist {
		// FIXME: always return 0?
		log.LogErrorf("Unregister app[%v] not exist: %v", appid)
		binary.BigEndian.PutUint64(ret, 0)
		return offs, ret
	}
	cid := app.Cid
	tid := app.Tid

	t, err := comm.CFSUnregisterTask(cid, tid)
	if err != nil {
		// FIXME: always return 0?
		log.LogErrorf("Unregister client[%v] task[%v]: %v", cid, tid, err)
		binary.BigEndian.PutUint64(ret, 0)
		return offs, ret
	}
	task := t.(*Task)
	fmt.Printf("DEBUG: task id %v unmap shm\n", tid)
	task.shutdown()
	syscall.Munmap(task.queueArray.ctrl.addr)
	syscall.Munmap(task.queueArray.data.addr)
	syscall.Munmap(task.queueArray.done.addr)

	if needCloseClient {
		fmt.Printf("DEBUG: client id %v close client\n", cid)
		comm.CFSCloseClient(cid)
	}

	binary.BigEndian.PutUint64(ret, 0)

	runtime.GC()

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
