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

#define _GNU_SOURCE
#include <dlfcn.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <mntent.h>
#include <unistd.h>
#include <errno.h>

#include "client.h"
#include "common.h"
#include "fd_map.h"
#include "apis.h"
#include "list.h"
#include "log.h"

#define __init __attribute__((constructor))
#define __exit __attribute__((destructor))

struct client_info *gci = NULL;

struct orig_apis orig_apis = {NULL};

#define STR(x) #x
#define SAVE_ORIG(_name) do {						\
	orig_apis._name = dlsym(RTLD_NEXT, STR(_name));			\
	if (orig_apis._name == NULL) {					\
		int err = -errno;					\
		pr_error("Failed to get orignal %s function: %s\n",	\
			 STR(_name), strerror(errno));			\
		return err;						\
	}								\
} while (0)

static int init_orig_apis(void)
{
	SAVE_ORIG(open);
	SAVE_ORIG(close);
	SAVE_ORIG(read);
	SAVE_ORIG(write);
	SAVE_ORIG(ftruncate);
	SAVE_ORIG(unlink);
	SAVE_ORIG(fork);
	SAVE_ORIG(vfork);
	SAVE_ORIG(clone);

	return 0;
}

static int get_cfs_mount_info(struct client_info *ci)
{
	const char *mounts = "/proc/mounts";
	struct mntent *mnt;
	FILE *f;
	bool found_cfs = false;
	int ret = 0;

	f = setmntent(mounts, "r");
	if (f == NULL) {
		ret = -errno;
		pr_error("Failed to open %s: %s\n", mounts, strerror(errno));
		return ret;
	}

	while (1) {
		mnt = getmntent(f);
		if (mnt == NULL)
			break;

		if (strcmp(mnt->mnt_type, ci->fstype))
			continue;

		/* found cfs mount point */
		pr_debug("fsname[%s] mnt_dir[%s] type[%s]\n",
			 mnt->mnt_fsname, mnt->mnt_dir, mnt->mnt_type);
		ret = append_mountpoint(ci, mnt->mnt_fsname, mnt->mnt_dir);
		if (ret != 0) {
			goto out;
		}

		found_cfs = true;
	}

	if (!found_cfs) {
		pr_error("CFS not found\n");
	}

out:
	endmntent(f);
	return ret;
}

static __init void init(void)
{
	pid_t pid = getpid();
	struct client_info *ci;
	struct open_fd *open_fd, *next;
	LIST_HEAD(open_fd_list);
	int ret;

	pr_debug("Start init for pid %d\n", pid);

	ret = init_orig_apis();
	if (ret < 0)
		goto out;

	ci = alloc_client(FSTYPE, pid);
	if (ci == NULL) {
		pr_error("Failed to create cfs client\n");
		goto out;
	}

	/* if no cfs mounted, keep cfs_mountpoints as null
	 * FIXME: but how to deal with dynamic mount after app is initialized?
	 */
	ret = get_cfs_mount_info(ci);
	if (ret < 0) {
		pr_error("Failed to get cfs mount points: %s\n", strerror(ret));
		goto close_out;
	}

	ret = append_fd_map_set(ci);
	if (ret < 0) {
		pr_error("Failed to create fd map: %s\n", strerror(ret));
		goto close_out;
	}

	ret = get_opened_fd(ci, &open_fd_list);
	if (ret < 0) {
		pr_error("Failed to get opened fds: %s\n", strerror(ret));
		goto close_out;
	}

	list_for_each_entry_safe(open_fd, next, &open_fd_list, link) {
		/* FIXME: map all opened fd as not in_cfs? */
		ret = map_fd(ci, NULL, open_fd->fd, open_fd->fd, -1);
		if (ret < 0) {
			pr_error("Failed to map fd: %s\n", strerror(ret));
			/* FIXME: need cleanup open_fd list */
			goto cleanup_fd_map_out;
		}

		free(open_fd);
	}

	ret = register_client(ci);
	if (ret < 0) {
		pr_error("Failed to register client: %s\n", strerror(ret));
		goto cleanup_fd_map_out;
	}

	gci = ci;

	return;

cleanup_fd_map_out:
	/* FIXME: cleanup will always be called when exit */
	//destroy_fd_map();
close_out:
	//close_cfs_client();
out:
	exit(ret);
}

static __exit void cleanup(void)
{
	pr_debug("Start cleanup all clients\n");
	destroy_all_clients();
}
