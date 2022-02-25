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
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/libsdk/comm"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	Name = "daemon"

	DefaultLogPath  = "/tmp/cfs"
	DefaultLogLevel = log.InfoLevel

	DefaultWorkspace = "/tmp/cfs/daemon"
	DefaultFileLock  = path.Join(DefaultWorkspace, "daemon.lock")

	DefaultPprofPort = 17520

	DefaultSockPath = path.Join(DefaultWorkspace, "daemon.sock")

	optVerbose = flag.Bool("V", false, "print debug log")
	optVersion = flag.Bool("v", false, "show version")
)

func Errno(err error) int {
	if err == nil {
		return 0
	}
	if errno, is := err.(syscall.Errno); is {
		return -int(errno)
	}
	log.LogErrorf("Unknown err: %v", err)
	return -int(syscall.EUCLEAN)
}

type App struct {
	Cid int64
	Tid int64
}

type Daemon struct {
	ctx    context.Context
	cancel context.CancelFunc

	flock *os.File

	listener net.Listener
	wg       *sync.WaitGroup

	nextAppId uint64
	apps      map[uint64]*App
	applock   sync.RWMutex
}

var daemon *Daemon = &Daemon{
	wg:   &sync.WaitGroup{},
	apps: make(map[uint64]*App),
}

func (d *Daemon) App(cid, tid int64) (uint64, *App) {
	appid := atomic.AddUint64(&d.nextAppId, 1)
	app := &App{Cid: cid, Tid: tid}
	return appid, app
}

func initLog() {
	logLevel := DefaultLogLevel
	if *optVerbose {
		logLevel = log.DebugLevel
	}
	if _, err := log.InitLog(DefaultLogPath, Name, logLevel, nil); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init log: %v\n", err)
		os.Exit(1)
	}
}

func checkEnv() {
	var err error

	// TODO: Check Environment variables to get chubaofs path

	// signal handler

	if _, err = os.Stat(DefaultWorkspace); err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(DefaultWorkspace, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create dir %s: %v\n", DefaultWorkspace, err)
			os.Exit(1)
		}
	}

	// check if sdk daemon already started
	if daemon.flock, err = os.OpenFile(DefaultFileLock, os.O_RDWR|os.O_CREATE, 0600); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file %s: %v\n", DefaultFileLock, err)
		os.Exit(1)
	}
	if err = syscall.Flock(int(daemon.flock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		daemon.flock.Close()
		fmt.Fprintf(os.Stderr, "Another daemon already locked %s: %v\n", DefaultFileLock, err)
		os.Exit(1)
	}
}

func cleanupEnv() {
	if daemon.cancel != nil {
		log.LogDebugf("context cancel is called\n")
		daemon.cancel()
	}

	if daemon.listener != nil && !reflect.ValueOf(daemon.listener).IsNil() {
		sockPath := daemon.listener.Addr().String()
		// unlink first to avoid new daemon acquire the same file
		syscall.Unlink(sockPath)
		daemon.listener.Close()
		log.LogDebugf("Remove %s\n", sockPath)
	}

	for appid, app := range daemon.apps {
		fmt.Printf("DEBUG: wait appid %v client[%v] task[%v]", appid, app.Cid, app.Tid)
		t, err := comm.CFSGetTask(app.Cid, app.Tid)
		if err != nil {
			fmt.Printf("Error: client[%v] task[%v]: %v", app.Cid, app.Tid, err)
			continue
		}
		fmt.Printf("DEBUG: wait client %v task %v", app.Cid, app.Tid)
		task := t.(*Task)
		task.wg.Wait()
	}

	if daemon.flock != nil {
		filePath := daemon.flock.Name()
		// unlink first to avoid new daemon acquire the same file
		syscall.Unlink(filePath)
		syscall.Flock(int(daemon.flock.Fd()), syscall.LOCK_UN)
		daemon.flock.Close()
		log.LogDebugf("Remove %s\n", filePath)
	}
}

func startPprofServer() {
	go func() {
		log.LogInfof("Start pprof with port %d", DefaultPprofPort)
		addr := fmt.Sprintf(":%d", DefaultPprofPort)
		http.ListenAndServe(addr, nil)
	}()
}

func registerSignalHandlers() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.LogInfof("signal %s received", sig.String())
		// TODO: wait events to finish and cleanup
		cleanupEnv()
		os.Exit(0)
	}()
}

func listenAndServe() (err error) {
	// FIXME: maybe use config to set the sock file path
	var laddr *net.UnixAddr

	sockAddr := DefaultSockPath

	fmt.Printf("DEBUG: create socket %s\n", sockAddr)
	if laddr, err = net.ResolveUnixAddr("unix", sockAddr); err != nil {
		log.LogErrorf("Cannot resolve unix addr %s: %v", sockAddr, err)
		return
	}

	if daemon.listener, err = net.ListenUnix("unix", laddr); err != nil {
		log.LogErrorf("Cannot create unix domain %s: %v", laddr.String(), err)
		return
	}

	if err = os.Chmod(sockAddr, 0666); err != nil {
		log.LogErrorf("Failed to chmod socket %s: %v", sockAddr, err)
		daemon.listener.Close()
		daemon.listener = nil
		return
	}

	for {
		var conn net.Conn
		if conn, err = daemon.listener.Accept(); err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			log.LogErrorf("Unix domain socket accepts fail: %v", err)
			continue
		}
		fmt.Printf("DEBUG: a new connection accepted\n")
		// FIXME: check request first? how to return error?
		daemon.wg.Add(1)
		go HandleCmd(daemon.wg, conn)
	}

	log.LogDebugf("goroutine: wait accept closed\n")
	daemon.wg.Wait()

	return
}

func main() {
	flag.Parse()

	if *optVersion {
		fmt.Fprintf(os.Stdout, proto.DumpVersion(Name))
		os.Exit(0)
	}

	daemon.ctx, daemon.cancel = context.WithCancel(context.Background())

	initLog()
	defer log.LogFlush()

	checkEnv()
	defer cleanupEnv()

	log.LogInfof("cfs daemon start")

	startPprofServer()

	registerSignalHandlers()

	if err := listenAndServe(); err != nil {
		log.LogErrorf("Failed to listen: %v", err)
		return
	}
}

type Task struct {
	Cid int64
	Tid int64

	queueArray *QueueArray
	QueueCh    chan interface{}
	ShutdownCh chan struct{}
	IdleCh     chan struct{}
	wg         sync.WaitGroup

	needExit bool
	isIdle   bool
}

func newTask(queueArray *QueueArray) *Task {
	task := &Task{
		QueueCh:    make(chan interface{}),
		ShutdownCh: make(chan struct{}),
		IdleCh:     make(chan struct{}),
	}

	task.queueArray = queueArray
	return task
}

func (task *Task) SetCid(cid int64) {
	task.Cid = cid
}

func (task *Task) SetTid(tid int64) {
	task.Tid = tid
}

func (task *Task) shutdown() {
	fmt.Printf("DEBUG: send shutdown\n")
	task.ShutdownCh <- struct{}{}
	// wait all goroutines to finish so that we could munmap shm safely
	task.wg.Wait()
}

func (task *Task) wakeup() {
	fmt.Printf("DEBUG: send wakeup\n")
	task.IdleCh <- struct{}{}
}

func (task *Task) serve(ctx context.Context) {
	task.wg.Add(1)
	go task.WaitQueueRequests(ctx)

	for {
		req, err := task.ReadRequest()
		if err != nil {
			fmt.Printf("DEBUG: Wait request handler to finish: %v\n", err)
			if err == io.EOF {
				break
			}
			// FIXME: should put errno in done item
			break
		}

		task.wg.Add(1)
		go func() {
			defer task.wg.Done()
			task.handleRequest(req)
		}()
	}

	fmt.Printf("DEBUG: goroutine: serve close\n")
	task.wg.Wait()
}

func (task *Task) WaitQueueRequests(ctx context.Context) {
	queueArray := task.queueArray

	defer func() {
		fmt.Printf("DEBUG: goroutine: Wait queue requests 1 close\n")
		task.wg.Done()
	}()

	idleChecker := time.NewTicker(time.Second)

	task.wg.Add(1)
	go func() {
		for {
			select {
			case <-idleChecker.C:
				if !task.isIdle {
					fmt.Printf("DEBUG: task id %d WaitQueueRequests became idle\n", task.Tid)
					task.isIdle = true
					flags := QueueSuspendFlag
					atomic.StoreUint32(queueArray.ctrl.hdr.flags, flags)
				}
			case <-ctx.Done():
				fmt.Printf("DEBUG: ctx is closed, set needExit true\n")
				task.needExit = true
			case <-task.ShutdownCh:
				fmt.Printf("DEBUG: task.ShutdownCh is closed, set needExit true\n")
				task.needExit = true
			}

			if task.needExit {
				idleChecker.Stop()
				if task.isIdle {
					task.IdleCh <- struct{}{}
				}
				fmt.Printf("DEBUG: goroutine: Wait queue requests 2 close\n")
				break
			}
		}
		task.wg.Done()
	}()

	for {
		if task.needExit {
			fmt.Printf("DEBUG: needExit is true, return\n")
			task.QueueCh <- io.EOF
			return
		}

		if task.isIdle {
			fmt.Printf("DEBUG: task id %d suspend polling ...\n", task.Tid)
			<-task.IdleCh
			atomic.StoreUint32(queueArray.ctrl.hdr.flags, 0)
			task.isIdle = false
			idleChecker.Reset(time.Second)
			fmt.Printf("DEBUG: task id %d reseum polling ...\n", task.Tid)
		}
		//fmt.Printf("DEBUG: Waiting queue requests ctrl size %v data size %v done size %v\n",
		//	unsafe.Sizeof(CtrlItem{}), unsafe.Sizeof(DataItem{}), unsafe.Sizeof(DoneItem{}))
		tail := atomic.LoadUint32(&queueArray.ctrl.tail)
		tail = tail & queueArray.ctrl.mask
		ctrlItem := queueArray.ctrl.QueueItem(tail).(*CtrlItem)
		//fmt.Printf("DEBUG: clientId %v tail %v CtrlItem %p ReqID:%v OpCode:%v State:%x DoneIdx:%v DataIdx:%v\n",
		//	client.id, tail, ctrlItem, ctrlItem.ReqId, ctrlItem.OpCode, ctrlItem.State, ctrlItem.DoneIdx, ctrlItem.DataIdx)
		if atomic.LoadUint32(&ctrlItem.State) == CtrlStateNew {
			//time.Sleep(time.Second)
			runtime.Gosched()
			continue
		}

		idleChecker.Reset(time.Second)
		atomic.AddUint32(&queueArray.ctrl.tail, 1)
		fmt.Printf("DEBUG: valid ctrl_item received, send to channel\n")
		task.QueueCh <- ctrlItem
	}
}

func (task *Task) ReadRequest() (*CtrlItem, error) {
	var data interface{}

	fmt.Printf("DEBUG: Start wait FuseCh and QueueCh\n")
	select {
	case data = <-task.QueueCh:
		fmt.Printf("DEBUG: QueueCh has request\n")
	}

	switch data.(type) {
	case error:
		err := data.(error)
		return nil, err
	default:
		return data.(*CtrlItem), nil
	}
}

func (task *Task) handleRequest(req *CtrlItem) {
	var (
		ret int64
		err error
	)

	fmt.Printf("DEBUG: handle request reqid %v dataIdx %v doneIdx %v opcode %v\n", req.ReqId, req.DataIdx, req.DoneIdx, req.OpCode)
	switch req.OpCode {
	case OPEN:
		var params *OpenParams
		if req.State&CtrlStateInlineData == CtrlStateInlineData {
			params = parseOpenParams(req.data[:], true)
		} else {
			params = parseOpenParams(req.data[:], false)
			// TODO read data item
		}

		filePath := string(params.path[:params.hdr.pathLen])
		fmt.Printf("DEBUG: flags:%x mode:%o pathLen:%v path:%s\n",
			params.hdr.flags, params.hdr.mode, params.hdr.pathLen, filePath)

		fd := comm.CFSOpen(task.Cid, filePath, params.hdr.flags, params.hdr.mode)
		ret = int64(fd)
		fmt.Printf("DEBUG: cfs_open %s return %v\n", filePath, ret)

	case CLOSE:
		var params *CloseParams = parseCloseParams(req.data[:])
		comm.CFSClose(task.Cid, params.fd)
		ret = 0 // never fail
		fmt.Printf("DEBUG: cfs_close %d return %v\n", params.fd, ret)

	case READ:
		var (
			params     *RWParams
			dataBuffer []*RWData
			left       int
			offs       int
			rsize      int
		)

		if req.State&CtrlStateInlineData == CtrlStateInlineData {
			params = parseRWParams(req.data[:], true)
		} else {
			params = parseRWParams(req.data[:], false)
		}
		fmt.Printf("DEBUG: fd %v size %v offs %v\n", params.hdr.fd, params.hdr.size, params.hdr.offs)
		if dataBuffer, err = parseRWData(req, task.queueArray.data, params); err != nil {
			log.LogErrorf("Unable to parse read data item: %v", err)
			ret = int64(Errno(syscall.EINVAL))
			break
		}
		fmt.Printf("DEBUG: dataBuffer entries: %v\n", len(dataBuffer))
		left = int(params.hdr.size)
		offs = int(params.hdr.offs)
		for _, buf := range dataBuffer {
			bufSize := util.Min(left, len(buf.data))
			fmt.Printf("DEBUG: read data size %v from offs %v\n", bufSize, offs)
			n := comm.CFSRead(task.Cid, int(params.hdr.fd), buf.data[offs:bufSize], offs)
			if n < 0 {
				log.LogErrorf("Failed to read %v", req.OpCode)
				fmt.Printf("Error: failed to read: %v\n", n)
				ret = int64(n)
				break
			} else if n < bufSize {
				fmt.Printf("DEBUG: data is not fully filled %v %s\n", n, string(buf.data))
				rsize += n
				break
			} else { // n == bufSize
				rsize += n
				left -= n
				offs += n
			}
		}
		if rsize > 0 {
			ret = int64(rsize)
		}
		fmt.Printf("DEBUG: read return %v\n", ret)

	case WRITE:
		var params *RWParams
		var dataBuffer []*RWData
		var left int
		var offs int
		var wsize int

		if req.State&CtrlStateInlineData == CtrlStateInlineData {
			params = parseRWParams(req.data[:], true)
		} else {
			params = parseRWParams(req.data[:], false)
		}
		if dataBuffer, err = parseRWData(req, task.queueArray.data, params); err != nil {
			log.LogErrorf("Unable to parse read data item: %v", err)
			ret = int64(Errno(syscall.EINVAL))
			break
		}
		left = int(params.hdr.size)
		offs = int(params.hdr.offs)
		for _, buf := range dataBuffer {
			n := comm.CFSWrite(task.Cid, int(params.hdr.fd), buf.data, offs)
			if n < 0 {
				log.LogErrorf("Failed to write %v", req.OpCode)
				fmt.Printf("Error: failed to read: %v\n", n)
				ret = int64(n)
				break
			} else if n < len(buf.data) {
				fmt.Printf("DEBUG: data is not fully writen: %v\n", n)
				wsize += n
				break
			} else { // n == bufSize
				wsize += n
				left -= n
				offs += n
			}
		}
		if wsize > 0 {
			ret = int64(wsize)
		}
		fmt.Printf("DEBUG: write return %v\n", ret)

	default:
		log.LogErrorf("Unknown opcode %v", req.OpCode)
		ret = int64(Errno(syscall.EINVAL))
	}

	doneItem := task.queueArray.done.QueueItem(req.DoneIdx).(*DoneItem)
	if doneItem.ReqId != req.ReqId || doneItem.OpCode != req.OpCode {
		panic(fmt.Sprintf("!!!ERROR!!!: invalid doneItem(%v) req(%v)\n", doneItem, req))
	}
	doneItem.QueueMarkItemDone(ret)
	fmt.Printf("Mark done item %v Done\n", req.DoneIdx)
}
