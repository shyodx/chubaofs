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
