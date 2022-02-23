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

	"github.com/chubaofs/chubaofs/util/log"
)

func HandleCmd(wg *sync.WaitGroup, conn net.Conn) {
	var (
		retData []byte
		rsize   int
		wsize   int
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

	retData = make([]byte, 8)
	binary.BigEndian.PutUint64(retData, 0)

	if wsize, err = conn.Write(retData); err != nil {
		log.LogErrorf("Failed to write conn: %v", err)
	}

	fmt.Printf("DEBUG: write %v bytes %v\n", wsize, retData)
}
