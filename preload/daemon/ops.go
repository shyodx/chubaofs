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
	"fmt"
	"reflect"
	"unsafe"
)

const (
	// Fuse defined OpCode
	OPEN  = 14

	// ChubaoFS self defined OpCode
	CLOSE     = 4095
)

type OpenParamsHdr struct {
	flags   uint32
	mode    uint32
	pathLen uint16
	rsvd1   uint16
	rsvd2   uint32
}

type OpenParams struct {
	hdr  *OpenParamsHdr
	path []byte
}

func parseOpenParams(data []byte, hasInlineData bool) *OpenParams {
	params := &OpenParams{}

	sliceHdr := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	paramsHdr := (*OpenParamsHdr)(unsafe.Pointer(sliceHdr.Data))
	fmt.Printf("Parse open params %v at addr %p\n", data, paramsHdr)
	params.hdr = paramsHdr
	if hasInlineData {
		sliceHdr = (*reflect.SliceHeader)(unsafe.Pointer(&params.path))
		sliceHdr.Data = uintptr(unsafe.Pointer(paramsHdr)) + uintptr(unsafe.Sizeof(*paramsHdr))
		sliceHdr.Len = int(params.hdr.pathLen)
		sliceHdr.Cap = int(params.hdr.pathLen)
	}

	return params
}

type CloseParams struct {
	fd int
}

func parseCloseParams(data []byte) *CloseParams {
	params := &CloseParams{}

	sliceHdr := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	params.fd = *((*int)(unsafe.Pointer(sliceHdr.Data)))

	return params
}
