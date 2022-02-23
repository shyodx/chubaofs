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

package comm

import (
	"io"
	"os"
	gopath "path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/willf/bitset"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	defaultBlkSize = uint32(1) << 12

	maxFdNum uint = 1024000
)

var gClientManager *clientManager = &clientManager{clients: make(map[int64]*client)}

var (
	statusOK = int(0)
	// error status must be minus value
	statusEIO     = errorToStatus(syscall.EIO)
	statusEINVAL  = errorToStatus(syscall.EINVAL)
	statusEEXIST  = errorToStatus(syscall.EEXIST)
	statusEBADFD  = errorToStatus(syscall.EBADFD)
	statusEACCES  = errorToStatus(syscall.EACCES)
	statusEMFILE  = errorToStatus(syscall.EMFILE)
	statusENOTDIR = errorToStatus(syscall.ENOTDIR)
	statusEISDIR  = errorToStatus(syscall.EISDIR)
)

func errorToStatus(err error) int {
	if err == nil {
		return 0
	}
	if errno, is := err.(syscall.Errno); is {
		return -int(errno)
	}
	return -int(syscall.EIO)
}

type clientManager struct {
	nextClientID int64
	clients      map[int64]*client
	mu           sync.RWMutex
}

func newClient() *client {
	cid := atomic.AddInt64(&gClientManager.nextClientID, 1)
	c := &client{
		cid:   cid,
		fdmap: make(map[uint]*file),
		fdset: bitset.New(maxFdNum),
		cwd:   "/",
		tasks: make(map[int64]UserTask),
	}

	gClientManager.mu.Lock()
	gClientManager.clients[cid] = c
	gClientManager.mu.Unlock()

	return c
}

func getClient(cid int64) (c *client, exist bool) {
	gClientManager.mu.RLock()
	defer gClientManager.mu.RUnlock()
	c, exist = gClientManager.clients[cid]
	return
}

func removeClient(cid int64) {
	gClientManager.mu.Lock()
	defer gClientManager.mu.Unlock()
	delete(gClientManager.clients, cid)
}

type file struct {
	fd    uint
	ino   uint64
	flags uint32
	mode  uint32

	// dir only
	dirp *dirStream
}

type dirStream struct {
	pos     int
	dirents []proto.Dentry
}

type UserTask interface {
	SetCid(cid int64)
	SetTid(tid int64)
}

type client struct {
	// client id allocated by libsdk
	cid int64

	// mount config
	volName      string
	masterAddr   string
	followerRead bool
	logDir       string
	logLevel     string
	userID       string
	accessKey    string
	secretKey    string

	// runtime context
	cwd    string // current working directory
	fdmap  map[uint]*file
	fdset  *bitset.BitSet
	fdlock sync.RWMutex

	// server info
	mw *meta.MetaWrapper
	ec *stream.ExtentClient

	// user tasks
	nextTaskID int64
	tasks      map[int64]UserTask
	taskLock   sync.RWMutex
}

func CFSNewClient() int64 {
	c := newClient()
	// Just skip fd 0, 1, 2, to avoid confusion.
	c.fdset.Set(0).Set(1).Set(2)
	return c.cid
}

func CFSSetClient(cid int64, key, val string) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}
	switch key {
	case "volName":
		c.volName = val
	case "masterAddr":
		c.masterAddr = val
	case "followerRead":
		if val == "true" {
			c.followerRead = true
		} else {
			c.followerRead = false
		}
	case "logDir":
		c.logDir = val
	case "logLevel":
		c.logLevel = val
	case "user":
		c.userID = val
	case "ak":
		c.accessKey = val
	case "sk":
		c.secretKey = val
	default:
		return statusEINVAL
	}
	return statusOK
}

func CFSStartClient(cid int64) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	err := c.start()
	if err != nil {
		return statusEIO
	}
	return statusOK
}

func CFSCloseClient(cid int64) {
	if c, exist := getClient(cid); exist {
		if c.ec != nil {
			_ = c.ec.Close()
		}
		if c.mw != nil {
			_ = c.mw.Close()
		}
		removeClient(cid)
	}
	log.LogFlush()
}

func CFSGetTask(cid, tid int64) (UserTask, error) {
	c, exist := getClient(cid)
	if !exist {
		return nil, errors.NewErrorf("client[%v] not found", cid)
	}

	c.taskLock.RLock()
	t, exist := c.tasks[tid]
	c.taskLock.RUnlock()

	if !exist {
		return nil, errors.NewErrorf("client[%v] task[%v] not found", cid, tid)
	}

	return t, nil
}

func CFSRegisterTask(cid int64, t UserTask) (int64, error) {
	c, exist := getClient(cid)
	if !exist {
		return 0, errors.NewErrorf("client[%v] not found", cid)
	}

	tid := atomic.AddInt64(&c.nextTaskID, 1)
	t.SetCid(cid)
	t.SetTid(tid)

	c.taskLock.Lock()
	c.tasks[tid] = t
	c.taskLock.Unlock()

	return tid, nil
}

func CFSUnregisterTask(cid, tid int64) (UserTask, error) {
	c, exist := getClient(cid)
	if !exist {
		return nil, errors.NewErrorf("client[%v] not found", cid)
	}

	c.taskLock.Lock()
	t, exist := c.tasks[tid]
	delete(c.tasks, tid)
	c.taskLock.Unlock()

	if !exist {
		return nil, errors.NewErrorf("client[%v] task[%v] not found", cid, tid)
	}

	return t, nil
}

func CFSChdir(cid int64, path string) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}
	cwd := c.absPath(path)
	dirInfo, err := c.lookupPath(cwd)
	if err != nil {
		return errorToStatus(err)
	}
	if !proto.IsDir(dirInfo.Mode) {
		return statusENOTDIR
	}
	c.cwd = cwd
	return statusOK
}

func CFSGetcwd(cid int64) string {
	c, exist := getClient(cid)
	if !exist {
		return ""
	}
	return c.cwd
}

type CFSStatInfo struct {
	Ino       uint64
	Size      uint64
	Blocks    uint64
	Atime     int64
	Mtime     int64
	Ctime     int64
	AtimeNsec uint32
	MtimeNsec uint32
	CtimeNsec uint32
	Mode      uint32
	Nlink     uint32
	BlkSize   uint32
	Uid       uint32
	Gid       uint32
}

func CFSGetattr(cid int64, path string) (*CFSStatInfo, int) {
	stat := &CFSStatInfo{}

	c, exist := getClient(cid)
	if !exist {
		return nil, statusEINVAL
	}

	info, err := c.lookupPath(c.absPath(path))
	if err != nil {
		return nil, errorToStatus(err)
	}

	// fill up the stat
	stat.Ino = info.Inode
	stat.Size = info.Size
	stat.Nlink = info.Nlink
	stat.BlkSize = defaultBlkSize
	stat.Uid = info.Uid
	stat.Gid = info.Gid
	stat.Mode = info.Mode

	if info.Size%512 != 0 {
		stat.Blocks = (info.Size >> 9) + 1
	} else {
		stat.Blocks = info.Size >> 9
	}

	t := info.AccessTime.UnixNano()
	stat.Atime = t / 1e9
	stat.AtimeNsec = uint32(t % 1e9)

	t = info.ModifyTime.UnixNano()
	stat.Mtime = t / 1e9
	stat.MtimeNsec = uint32(t % 1e9)

	t = info.CreateTime.UnixNano()
	stat.Ctime = t / 1e9
	stat.CtimeNsec = uint32(t % 1e9)

	return stat, statusOK
}

func CFSSetattr(cid int64, path string, stat *CFSStatInfo, valid uint32) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	info, err := c.lookupPath(c.absPath(path))
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(info, valid, stat.Mode, stat.Uid, stat.Gid, stat.Atime, stat.Mtime)

	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

func CFSOpen(cid int64, path string, flags, mode uint32) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	fuseMode := uint32(mode) & uint32(0777)
	fuseFlags := uint32(flags) &^ uint32(0x8000)
	accFlags := fuseFlags & uint32(syscall.O_ACCMODE)

	absPath := c.absPath(path)

	var info *proto.InodeInfo

	/*
	 * Note that the rwx mode is ignored when using libsdk
	 */

	if fuseFlags&uint32(os.O_CREATE) != 0 {
		if accFlags != uint32(os.O_WRONLY) && accFlags != uint32(os.O_RDWR) {
			return statusEACCES
		}
		dirpath, name := gopath.Split(absPath)
		dirInfo, err := c.lookupPath(dirpath)
		if err != nil {
			return errorToStatus(err)
		}
		newInfo, err := c.create(dirInfo.Inode, name, fuseMode)
		if err != nil {
			if err != syscall.EEXIST {
				return errorToStatus(err)
			}
			newInfo, err = c.lookupPath(absPath)
			if err != nil {
				return errorToStatus(err)
			}
		}
		info = newInfo
	} else {
		newInfo, err := c.lookupPath(absPath)
		if err != nil {
			return errorToStatus(err)
		}
		info = newInfo
	}

	f := c.allocFD(info.Inode, fuseFlags, fuseMode)
	if f == nil {
		return statusEMFILE
	}

	if proto.IsRegular(info.Mode) {
		c.openStream(f)
		if fuseFlags&uint32(os.O_TRUNC) != 0 {
			if accFlags != uint32(os.O_WRONLY) && accFlags != uint32(os.O_RDWR) {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return statusEACCES
			}
			if err := c.truncate(f, 0); err != nil {
				c.closeStream(f)
				c.releaseFD(f.fd)
				return statusEIO
			}
		}
	}

	return int(f.fd)
}

func CFSFlush(cid int64, fd int) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	err := c.flush(f)
	if err != nil {
		return statusEIO
	}
	return statusOK
}

func CFSClose(cid int64, fd int) {
	c, exist := getClient(cid)
	if !exist {
		return
	}
	f := c.releaseFD(uint(fd))
	if f != nil {
		c.flush(f)
		c.closeStream(f)
	}
}

func CFSWrite(cid int64, fd int, buffer []byte, off int) int {
	var (
		flags int
		wait  bool
	)

	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	accFlags := f.flags & uint32(syscall.O_ACCMODE)
	if accFlags != uint32(os.O_WRONLY) && accFlags != uint32(os.O_RDWR) {
		return statusEACCES
	}

	if f.flags&uint32(syscall.O_DIRECT) != 0 ||
		f.flags&uint32(os.O_SYNC) != 0 ||
		f.flags&uint32(syscall.O_DSYNC) != 0 {
		wait = true
	}
	if f.flags&uint32(os.O_APPEND) != 0 {
		flags |= proto.FlagsAppend
	}

	n, err := c.write(f, off, buffer, flags)
	if err != nil {
		return statusEIO
	}

	if wait {
		if err = c.flush(f); err != nil {
			return statusEIO
		}
	}

	return n
}

func CFSRead(cid int64, fd int, buffer []byte, off int) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	accFlags := f.flags & uint32(syscall.O_ACCMODE)
	if accFlags == uint32(os.O_WRONLY) {
		return statusEACCES
	}

	n, err := c.read(f, off, buffer)
	if err != nil {
		return statusEIO
	}

	return n
}

type CFSDirent struct {
	Ino   uint64
	Name  string
	DType uint32
}

func CFSReaddir(cid int64, fd int, count int) ([]*CFSDirent, int) {
	var (
		n       int
		dirents = make([]*CFSDirent, 0)
	)

	c, exist := getClient(cid)
	if !exist {
		return nil, statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return nil, statusEBADFD
	}

	if f.dirp == nil {
		f.dirp = &dirStream{}
		dentries, err := c.mw.ReadDir_ll(f.ino)
		if err != nil {
			return nil, errorToStatus(err)
		}
		f.dirp.dirents = dentries
	}

	dirp := f.dirp
	for dirp.pos < len(dirp.dirents) && n < count {
		ent := &CFSDirent{}
		ent.Ino = dirp.dirents[dirp.pos].Inode
		ent.DType = dirp.dirents[dirp.pos].Type
		ent.Name = dirp.dirents[dirp.pos].Name
		dirents = append(dirents, ent)

		dirp.pos++
		n++
	}

	return dirents, n
}

func CFSMkdirs(cid int64, path string, mode uint32) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	dirpath := c.absPath(path)
	if dirpath == "/" {
		return statusEEXIST
	}

	pino := proto.RootIno
	dirs := strings.Split(dirpath, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}
		child, _, err := c.mw.Lookup_ll(pino, dir)
		if err != nil {
			if err == syscall.ENOENT {
				info, err := c.mkdir(pino, dir, uint32(mode))
				if err != nil {
					return errorToStatus(err)
				}
				child = info.Inode
			} else {
				return errorToStatus(err)
			}
		}
		pino = child
	}

	return 0
}

func CFSRmdir(cid int64, path string) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(path)
	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	_, err = c.mw.Delete_ll(dirInfo.Inode, name, true)
	return errorToStatus(err)
}

func CFSUnlink(cid int64, path string) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	absPath := c.absPath(path)
	dirpath, name := gopath.Split(absPath)
	dirInfo, err := c.lookupPath(dirpath)
	if err != nil {
		return errorToStatus(err)
	}

	_, mode, err := c.mw.Lookup_ll(dirInfo.Inode, name)
	if proto.IsDir(mode) {
		return statusEISDIR
	}

	info, err := c.mw.Delete_ll(dirInfo.Inode, name, false)
	if err != nil {
		return errorToStatus(err)
	}

	_ = c.mw.Evict(info.Inode)
	return 0
}

func CFSRename(cid int64, from, to string) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	absFrom := c.absPath(from)
	absTo := c.absPath(to)
	srcDirPath, srcName := gopath.Split(absFrom)
	dstDirPath, dstName := gopath.Split(absTo)

	srcDirInfo, err := c.lookupPath(srcDirPath)
	if err != nil {
		return errorToStatus(err)
	}
	dstDirInfo, err := c.lookupPath(dstDirPath)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.mw.Rename_ll(srcDirInfo.Inode, srcName, dstDirInfo.Inode, dstName)
	return errorToStatus(err)
}

func CFSFchmod(cid int64, fd int, mode uint32) int {
	c, exist := getClient(cid)
	if !exist {
		return statusEINVAL
	}

	f := c.getFile(uint(fd))
	if f == nil {
		return statusEBADFD
	}

	info, err := c.mw.InodeGet_ll(f.ino)
	if err != nil {
		return errorToStatus(err)
	}

	err = c.setattr(info, proto.AttrMode, mode, 0, 0, 0, 0)
	if err != nil {
		return errorToStatus(err)
	}
	return statusOK
}

// internals

func (c *client) absPath(path string) string {
	p := gopath.Clean(path)
	if !gopath.IsAbs(p) {
		p = gopath.Join(c.cwd, p)
	}
	return gopath.Clean(p)
}

func (c *client) start() (err error) {
	var masters = strings.Split(c.masterAddr, ",")

	if c.logDir != "" {
		log.InitLog(c.logDir, "libcfs", log.InfoLevel, nil)
	}

	var mw *meta.MetaWrapper
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        c.volName,
		Masters:       masters,
		ValidateOwner: false,
		Owner:         c.userID,
		AccessKey:     c.accessKey,
		SecretKey:     c.secretKey,
	}); err != nil {
		return
	}

	var ec *stream.ExtentClient
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            c.volName,
		Masters:           masters,
		FollowerRead:      c.followerRead,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
	}); err != nil {
		return
	}

	c.mw = mw
	c.ec = ec
	return nil
}

func (c *client) allocFD(ino uint64, flags, mode uint32) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	fd, ok := c.fdset.NextClear(0)
	if !ok || fd > maxFdNum {
		return nil
	}
	c.fdset.Set(fd)
	f := &file{fd: fd, ino: ino, flags: flags, mode: mode}
	c.fdmap[fd] = f
	return f
}

func (c *client) getFile(fd uint) *file {
	c.fdlock.Lock()
	f := c.fdmap[fd]
	c.fdlock.Unlock()
	return f
}

func (c *client) releaseFD(fd uint) *file {
	c.fdlock.Lock()
	defer c.fdlock.Unlock()
	f, ok := c.fdmap[fd]
	if !ok {
		return nil
	}
	delete(c.fdmap, fd)
	c.fdset.Clear(fd)
	return f
}

func (c *client) lookupPath(path string) (*proto.InodeInfo, error) {
	ino, err := c.mw.LookupPath(gopath.Clean(path))
	if err != nil {
		return nil, err
	}
	info, err := c.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (c *client) setattr(info *proto.InodeInfo, valid uint32, mode, uid, gid uint32, atime, mtime int64) error {
	// Only rwx mode bit can be set
	if valid&proto.AttrMode != 0 {
		fuseMode := mode & uint32(0777)
		mode = info.Mode &^ uint32(0777) // clear rwx mode bit
		mode |= fuseMode
	}

	return c.mw.Setattr(info.Inode, valid, mode, uid, gid, atime, mtime)
}

func (c *client) create(pino uint64, name string, mode uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil)
}

func (c *client) mkdir(pino uint64, name string, mode uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return c.mw.Create_ll(pino, name, fuseMode, 0, 0, nil)
}

func (c *client) openStream(f *file) {
	_ = c.ec.OpenStream(f.ino)
}

func (c *client) closeStream(f *file) {
	_ = c.ec.CloseStream(f.ino)
	_ = c.ec.EvictStream(f.ino)
}

func (c *client) flush(f *file) error {
	return c.ec.Flush(f.ino)
}

func (c *client) truncate(f *file, size int) error {
	return c.ec.Truncate(f.ino, size)
}

func (c *client) write(f *file, offset int, data []byte, flags int) (n int, err error) {
	n, err = c.ec.Write(f.ino, offset, data, flags)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *client) read(f *file, offset int, data []byte) (n int, err error) {
	n, err = c.ec.Read(f.ino, data, offset, len(data))
	if err != nil && err != io.EOF {
		return 0, err
	}
	return n, nil
}
