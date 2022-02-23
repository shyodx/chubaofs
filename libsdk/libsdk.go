// Copyright 2020 The ChubaoFS Authors.
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

/*

#define _GNU_SOURCE
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>

struct cfs_stat_info {
    uint64_t ino;
    uint64_t size;
    uint64_t blocks;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint32_t atime_nsec;
    uint32_t mtime_nsec;
    uint32_t ctime_nsec;
    mode_t   mode;
    uint32_t nlink;
    uint32_t blk_size;
    uint32_t uid;
    uint32_t gid;
};

struct cfs_dirent {
    uint64_t ino;
    char     name[256];
	char     d_type;
};

*/
import "C"

import (
	"reflect"
	"unsafe"

	"github.com/chubaofs/chubaofs/libsdk/comm"
	"github.com/chubaofs/chubaofs/proto"
)

//export cfs_new_client
func cfs_new_client() C.int64_t {
	cid := comm.CFSNewClient()
	return C.int64_t(cid)
}

//export cfs_set_client
func cfs_set_client(id C.int64_t, key, val *C.char) C.int {
	ret := comm.CFSSetClient(int64(id), C.GoString(key), C.GoString(val))
	return C.int(ret)
}

//export cfs_start_client
func cfs_start_client(id C.int64_t) C.int {
	ret := comm.CFSStartClient(int64(id))
	return C.int(ret)
}

//export cfs_close_client
func cfs_close_client(id C.int64_t) {
	comm.CFSCloseClient(int64(id))
}

//export cfs_chdir
func cfs_chdir(id C.int64_t, path *C.char) C.int {
	ret := comm.CFSChdir(int64(id), C.GoString(path))
	return C.int(ret)
}

//export cfs_getcwd
func cfs_getcwd(id C.int64_t) *C.char {
	cwd := comm.CFSGetcwd(int64(id))
	return C.CString(cwd)
}

//export cfs_getattr
func cfs_getattr(id C.int64_t, path *C.char, stat *C.struct_cfs_stat_info) C.int {
	st, ret := comm.CFSGetattr(int64(id), C.GoString(path))
	if ret < 0 {
		return C.int(ret)
	}

	stat.ino = C.uint64_t(st.Ino)
	stat.size = C.uint64_t(st.Size)
	stat.nlink = C.uint32_t(st.Nlink)
	stat.blk_size = C.uint32_t(st.BlkSize)
	stat.uid = C.uint32_t(st.Uid)
	stat.gid = C.uint32_t(st.Gid)
	stat.blocks = C.uint64_t(st.Size)

	if proto.IsRegular(st.Mode) {
		stat.mode = C.uint32_t(C.S_IFREG) | C.uint32_t(st.Mode&0777)
	} else if proto.IsDir(st.Mode) {
		stat.mode = C.uint32_t(C.S_IFDIR) | C.uint32_t(st.Mode&0777)
	} else if proto.IsSymlink(st.Mode) {
		stat.mode = C.uint32_t(C.S_IFLNK) | C.uint32_t(st.Mode&0777)
	} else {
		stat.mode = C.uint32_t(C.S_IFSOCK) | C.uint32_t(st.Mode&0777)
	}

	stat.atime = C.uint64_t(st.Atime)
	stat.atime_nsec = C.uint32_t(st.AtimeNsec)
	stat.mtime = C.uint64_t(st.Mtime)
	stat.mtime_nsec = C.uint32_t(st.MtimeNsec)
	stat.ctime = C.uint64_t(st.Ctime)
	stat.ctime_nsec = C.uint32_t(st.CtimeNsec)

	return C.int(ret)
}

//export cfs_setattr
func cfs_setattr(id C.int64_t, path *C.char, stat *C.struct_cfs_stat_info, valid C.int) C.int {
	st := &comm.CFSStatInfo{
		Mode:  uint32(stat.mode),
		Uid:   uint32(stat.uid),
		Gid:   uint32(stat.gid),
		Atime: int64(stat.atime),
		Mtime: int64(stat.mtime),
	}

	ret := comm.CFSSetattr(int64(id), C.GoString(path), st, uint32(valid))
	return C.int(ret)
}

//export cfs_open
func cfs_open(id C.int64_t, path *C.char, flags C.int, mode C.mode_t) C.int {
	fd := comm.CFSOpen(int64(id), C.GoString(path), uint32(flags), uint32(mode))
	return C.int(fd)
}

//export cfs_flush
func cfs_flush(id C.int64_t, fd C.int) C.int {
	ret := comm.CFSFlush(int64(id), int(fd))
	return C.int(ret)
}

//export cfs_close
func cfs_close(id C.int64_t, fd C.int) {
	comm.CFSClose(int64(id), int(fd))
}

//export cfs_write
func cfs_write(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) C.ssize_t {
	var buffer []byte

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	ssize := comm.CFSWrite(int64(id), int(fd), buffer, int(off))
	return C.ssize_t(ssize)
}

//export cfs_read
func cfs_read(id C.int64_t, fd C.int, buf unsafe.Pointer, size C.size_t, off C.off_t) C.ssize_t {
	var buffer []byte

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buffer))
	hdr.Data = uintptr(buf)
	hdr.Len = int(size)
	hdr.Cap = int(size)

	ssize := comm.CFSRead(int64(id), int(fd), buffer, int(off))
	return C.ssize_t(ssize)
}

/*
 * Note that readdir is not thread-safe according to the POSIX spec.
 */

//export cfs_readdir
func cfs_readdir(id C.int64_t, fd C.int, dirents []C.struct_cfs_dirent, count C.int) C.int {
	ents, n := comm.CFSReaddir(int64(id), int(fd), int(count))

	if n <= 0 {
		return C.int(n)
	}

	for i, ent := range ents {
		dirents[i].ino = C.uint64_t(ent.Ino)
		if proto.IsRegular(ent.DType) {
			dirents[i].d_type = C.DT_REG
		} else if proto.IsDir(ent.DType) {
			dirents[i].d_type = C.DT_DIR
		} else if proto.IsSymlink(ent.DType) {
			dirents[i].d_type = C.DT_LNK
		} else {
			dirents[i].d_type = C.DT_UNKNOWN
		}

		nameLen := len(ent.Name)
		if nameLen >= 256 {
			nameLen = 255
		}

		hdr := (*reflect.StringHeader)(unsafe.Pointer(&ent.Name))
		C.memcpy(unsafe.Pointer(&dirents[i].name[0]), unsafe.Pointer(hdr.Data), C.size_t(nameLen))
		dirents[i].name[nameLen] = 0
	}

	return C.int(n)
}

//export cfs_mkdirs
func cfs_mkdirs(id C.int64_t, path *C.char, mode C.mode_t) C.int {
	ret := comm.CFSMkdirs(int64(id), C.GoString(path), uint32(mode))
	return C.int(ret)
}

//export cfs_rmdir
func cfs_rmdir(id C.int64_t, path *C.char) C.int {
	ret := comm.CFSRmdir(int64(id), C.GoString(path))
	return C.int(ret)
}

//export cfs_unlink
func cfs_unlink(id C.int64_t, path *C.char) C.int {
	ret := comm.CFSUnlink(int64(id), C.GoString(path))
	return C.int(ret)
}

//export cfs_rename
func cfs_rename(id C.int64_t, from *C.char, to *C.char) C.int {
	ret := comm.CFSRename(int64(id), C.GoString(from), C.GoString(to))
	return C.int(ret)
}

//export cfs_fchmod
func cfs_fchmod(id C.int64_t, fd C.int, mode C.mode_t) C.int {
	ret := comm.CFSFchmod(int64(id), int(fd), uint32(mode))
	return C.int(ret)
}

func main() {}
