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

#ifndef _CFS_PRELOAD_LIB_CLIENT_H
#define _CFS_PRELOAD_LIB_CLIENT_H

#include <pthread.h>
#include <stdatomic.h>
#include <inttypes.h>

#include "list.h"
#include "queue.h"

extern struct client_info *gci;

#define CI_FLAG_NEW	0
#define CI_FLAG_CLONE	(1U << 0)
#define CI_FLAG_READY	(1U << 1)

struct cfs_config {
	char *master_addr;
	char *volname;
	char *log_dir;
	char *log_level;
	char *follower_read;
};

struct fsname_dir {
	int offs;
	char data[0];
};

/* Each mountpoint could have one cfs_client */
/* FIXME: we should provide an ioctl to get server info, so that we could
 * create an cfs client and config libsdk */
struct mountpoint {
	int64_t cid; // FIXME: is not used, we do not pass cid to libsdk daemon
	atomic_int refcnt;
	struct cfs_config config;
	struct list_head mountpoint_link;
	struct queue_info *queue_array[QUEUE_TYPE_NR];
	struct fsname_dir fsname_dir;
};

#define MNT_FSNAME(mnt) ((mnt)->fsname_dir.data + (mnt)->fsname_dir.offs)
#define MNT_DIR(mnt) ((mnt)->fsname_dir.data)

struct client_info {
	pthread_rwlock_t rwlock;
	pid_t pid;
	int64_t appid;
	atomic_int refcnt;
	unsigned int flags;
	unsigned int fd_map_set_nr;
	unsigned int total_free_fd_nr;
	struct list_head fd_map_set_list;
	struct list_head mountpoint_list;

	char fstype[0]; /* KEEP IT AS THE LAST ELEMENT */
};

struct client_info *alloc_client(const char *fstype, pid_t pid);
int register_client(struct client_info *ci);
int append_mountpoint(struct client_info *ci, const char *mnt_fsname, const char *mnt_dir);

#endif
