#ifndef _CFS_PRELOAD_LIB_APIS_H
#define _CFS_PRELOAD_LIB_APIS_H

#include <sys/types.h>
#include <stdint.h>
#include <unistd.h>

/* the same as vendor/bazil.org/fuse/fuse_kernel.go */
#define CBFS_LOOKUP		1
#define CBFS_GETATTR		3
#define CBFS_SETATTR		4
#define CBFS_READLINK		5
#define CBFS_SYMLINK		6
#define CBFS_MKNOD		8
#define CBFS_MKDIR		9
#define CBFS_UNLINK		10
#define CBFS_RMDIR		11
#define CBFS_RENAME		12
#define CBFS_LINK		13
#define CBFS_OPEN		14
#define CBFS_READ		15
#define CBFS_WRITE		16
#define CBFS_STATFS		17
#define CBFS_RELEASE		18
#define CBFS_FSYNC		20
#define CBFS_SETXATTR		21
#define CBFS_GETXATTR		22
#define CBFS_LISTXATTR		23
#define CBFS_REMOVEXATTR	24
#define CBFS_FLUSH		25
#define CBFS_INIT		26
#define CBFS_OPENDIR		27
#define CBFS_READDIR		28
#define CBFS_RELEASEDIR		29
#define CBFS_FSYNCDIR		30
#define CBFS_GETLK		31
#define CBFS_SETLK		32
#define CBFS_SETLKW		33
#define CBFS_ACCESS		34
#define CBFS_CREATE		35
#define CBFS_INTERRUPT		36
#define CBFS_BMAP		37
#define CBFS_DESTROY		38
#define CBFS_IOCTL		39 // Linux?
#define CBFS_POLL		40 // Linux?
#define CBFS_NOTIFY_REPLY	41
#define CBFS_BATCH_FORGET	42
#define CBFS_FALLOCATE		43
#define CBFS_READDIRPLUS	44
#define CBFS_RENAME2		45
#define CBFS_LSEEK		46
#define CBFS_COPY_FILE_RANGE	47

/* self defined opcode */
#define CBFS_CLOSE		4095
#define CBFS_HOLD_FILE		4094

struct orig_apis {
	int(*open)(const char *pathname, int flags, ...);
	int(*close)(int fd);
	ssize_t(*read)(int fd, void *buf, size_t count);
	ssize_t(*write)(int fd, const void *buf, size_t count);
	int(*ftruncate)(int fd, off_t length);
	int(*unlink)(const char *path);
};

extern struct orig_apis orig_apis;

static uint64_t reqid;

static uint64_t inline next_req_id(void)
{
	//return atomic_fetch_add(reqid, 1);
	//FIXME: need atomic operation
	return ++reqid;
}

#endif
