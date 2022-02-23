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
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"syscall"

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

	optVerbose = flag.Bool("V", false, "print debug log")
	optVersion = flag.Bool("v", false, "show version")
)

type Daemon struct {
	ctx    context.Context
	cancel context.CancelFunc

	flock *os.File
}

var daemon *Daemon = &Daemon{}

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
}
