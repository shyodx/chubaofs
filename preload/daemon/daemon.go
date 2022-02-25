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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/chubaofs/chubaofs/libsdk/comm"
	"github.com/chubaofs/chubaofs/proto"
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
	wg         sync.WaitGroup
}

func newTask(queueArray *QueueArray) *Task {
	task := &Task{}

	task.queueArray = queueArray
	return task
}

func (task *Task) SetCid(cid int64) {
	task.Cid = cid
}

func (task *Task) SetTid(tid int64) {
	task.Tid = tid
}

func (task *Task) serve(ctx context.Context) {
}
