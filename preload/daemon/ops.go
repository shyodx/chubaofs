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

	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	// Fuse defined OpCode
	OPEN  = 14
	READ  = 15

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

type RWParamsHdr struct {
	fd   uint32
	rsvd uint32
	size uint64
	offs uint64
}

type RWParams struct {
	hdr  *RWParamsHdr
	data []byte
}

type RWDataHdr struct {
	reqId  uint64
	opCode uint32
	next   uint32
}

type RWData struct {
	hdr  *RWDataHdr
	data []byte
}

func parseRWParams(data []byte, hasInlineData bool) *RWParams {
	params := &RWParams{}

	sliceHdr := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	paramsHdr := (*RWParamsHdr)(unsafe.Pointer(sliceHdr.Data))
	params.hdr = paramsHdr
	if hasInlineData {
		sliceHdr = (*reflect.SliceHeader)(unsafe.Pointer(&params.data))
		sliceHdr.Data = uintptr(unsafe.Pointer(paramsHdr)) + uintptr(unsafe.Sizeof(*paramsHdr))
		sliceHdr.Len = int(params.hdr.size)
		sliceHdr.Cap = int(params.hdr.size)
	}

	return params
}

func parseRWData(ctrlItem *CtrlItem, dataQueue *QueueInfo, params *RWParams) ([]*RWData, error) {
	var dataBuffer []*RWData = make([]*RWData, 0)

	size := int(params.hdr.size)
	if ctrlItem.State&CtrlStateInlineData == CtrlStateInlineData {
		var dataHdr *RWDataHdr = &RWDataHdr{
			reqId:  ctrlItem.ReqId,
			opCode: ctrlItem.OpCode,
			next:   DataSlotEnd,
		}
		var data *RWData = &RWData{hdr: dataHdr}
		sliceHdr := (*reflect.SliceHeader)(unsafe.Pointer(&data.data))
		sliceHdr.Data = uintptr(unsafe.Pointer(&ctrlItem.data[0])) + uintptr(unsafe.Sizeof(RWParamsHdr{}))
		sliceHdr.Cap = size
		sliceHdr.Len = size

		dataBuffer = append(dataBuffer, data)
		fmt.Printf("DEBUG: Inline data size %v %s\n", size, string(data.data))
	} else {
		idx := ctrlItem.DataIdx
		item := dataQueue.QueueItem(idx)
		for item != nil {
			dataItem := item.(*DataItem)
			var dataHdr *RWDataHdr = &RWDataHdr{
				reqId:  dataItem.ReqId,
				opCode: dataItem.OpCode,
				next:   dataItem.Next,
			}
			var data *RWData = &RWData{hdr: dataHdr}
			sliceHdr := (*reflect.SliceHeader)(unsafe.Pointer(&data.data))
			sliceHdr.Data = uintptr(unsafe.Pointer(&dataItem.data[0]))
			sliceHdr.Cap = util.Min(size, DataDataSize)
			sliceHdr.Len = util.Min(size, DataDataSize)

			dataBuffer = append(dataBuffer, data)

			idx = dataItem.Next
			item = dataQueue.QueueItem(idx)
			size -= sliceHdr.Cap
			if size == 0 {
				if item != nil {
					fmt.Printf("Error: size is zero but still have data item\n")
					log.LogWarnf("size is zero but still have data item")
				}
				break
			}
		}
	}

	return dataBuffer, nil
}
