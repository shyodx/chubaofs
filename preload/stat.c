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

#include <unistd.h>
#include <errno.h>

#if 0
int fstat(int fd, struct stat *statbuf)
{
	struct client_info *ci = get_client(getpid());
	struct cfs_stat_info csi = {0};
	struct fd_map map = {0};
	int ret;

	ret = query_fd(fd, &map);
	if (ret < 0) {
		pr_error("Failed to query fd %d: %s\n", fd, strerror(ret));
		goto out;
	}

	if (IS_CFS_FD(&map)) {
		ret = cfs_getattr(map->cid, map.real_fd, &csi);
		if (ret < 0) {
			goto out;
		}

		/* no need to check if statbuf is null */
		statbuf.st_dev = (dev_t)0; // FIXME
		statbuf.st_ino = (ino_t)csi.ino;
		statbuf.st_mode = csi.mode;
		statbuf.st_nlink = (nlink_t)csi.nlink;
		statbuf.st_uid = (uid_t)csi.uid;
		statbuf.st_gid = (gid_t)csi.gid;
		statbuf.st_rdev = (dev_t)0; // FIXME
		statbuf.st_size = (off_t)size;
		statbuf.st_blksize = (blksize_t)blk_size;
		statbuf.st_blocks = (blkcnt_t)blocks;
		statbuf.st_atim.tv_sec = atime;
		statbuf.st_mtim.tv_sec = mtime;
		statbuf.st_ctim.tv_sec = ctime;
		statbuf.st_atim.tv_nsec = atime_nsec;
		statbuf.st_mtim.tv_nsec = mtime_nsec;
		statbuf.st_ctim.tv_nsec = ctime_nsec;
	} else {
		ret = orig_apis.fstat(map.real_fd, statbuf);
	}

out:
	put_client(ci);
	return ret;
}
#endif

int ftruncate(int fd, off_t length)
{
	return -ENOSYS;
}
