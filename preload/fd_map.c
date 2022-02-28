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
//

#include <pthread.h>
#include <unistd.h>
#include <dirent.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>

#include <sys/types.h>

#include "common.h"
#include "client.h"
#include "fd_map.h"
#include "queue.h"
#include "log.h"

#define FILE_PATH_LEN 128

#define AUTO_SELECT -1

static struct fd_map_set *do_append_fd_map_set(struct client_info *ci, int start)
{

	struct fd_map_set *fds, *next;
	int start_fd;

	fds = malloc(sizeof(struct fd_map_set));
	if (fds == NULL)
		return NULL;

	for (int i = 0; i < FD_PER_SET; i++) {
		fds->fd_maps[i].real_fd = -1;
		fds->fd_maps[i].cid = -1;
	}

	if (start >= 0) {
		start_fd = (start / FD_PER_SET) * FD_PER_SET;
		list_for_each_entry(next, &ci->fd_map_set_list, fds_link) {
			if (start_fd > next->start_fd) {
				assert(start_fd >= next->start_fd + FD_PER_SET);
				continue;
			}
			break;
		}
	} else {
		start_fd = 0;
		list_for_each_entry(next, &ci->fd_map_set_list, fds_link) {
			if (start_fd >= next->start_fd) {
				start_fd += FD_PER_SET;
				continue;
			}
			break;
		}
	}

	/* sorted fds, add fds before `next' */
	INIT_LIST_HEAD(&fds->fds_link);
	fds->start_fd = start_fd;
	fds->free_nr = FD_PER_SET;
	list_add_tail(&fds->fds_link, &next->fds_link);
	ci->fd_map_set_nr++;
	ci->total_free_fd_nr += FD_PER_SET;

	pr_debug("Alloc new fd_map_set start_fd %d, fd_map_set_nr %u total_free_fd_nr %u\n",
		 fds->start_fd, ci->fd_map_set_nr, ci->total_free_fd_nr);

	return fds;
}

int append_fd_map_set(struct client_info *ci)
{
	struct fd_map_set *fds;

	/* FIXME: its safe to not hold lock here */
	pthread_rwlock_wrlock(&ci->rwlock);
	fds = do_append_fd_map_set(ci, AUTO_SELECT);
	pthread_rwlock_unlock(&ci->rwlock);
	if (fds == NULL)
		return -1;

	return 0;
}

int get_opened_fd(struct client_info *ci, struct list_head *head)
{
	pid_t pid = getpid();
	char opened_fd_path[FILE_PATH_LEN] = {0};
	DIR *dir;
	struct dirent *ent;
	int ret;

	ret = snprintf(opened_fd_path, FILE_PATH_LEN, "/proc/%d/fd", pid);
	if (ret < 0 || ret == FILE_PATH_LEN) {
		pr_error("opened_fd_path too long for pid %d\n", pid);
		return -ENAMETOOLONG;
	}

	dir = opendir(opened_fd_path);
	if (dir == NULL) {
		ret = -errno;
		pr_error("Failed to opendir %s: %s\n", opened_fd_path, strerror(errno));
		return ret;
	}

	errno = 0;
	while (1) {
		ent = readdir(dir);
		if (ent == NULL) {
			ret = -errno;
			break;
		}

		if (!strcmp(ent->d_name, dot) || !strcmp(ent->d_name, dotdot))
			continue;

		pr_debug("Get opened fd %s\n", ent->d_name);
		struct open_fd *fd = malloc(sizeof(struct open_fd));
		if (fd == NULL) {
			ret = -ENOMEM;
			break;
		}

		fd->fd = (int)strtol(ent->d_name, NULL, 0);
		if (errno != 0 || fd->fd == LONG_MAX || fd->fd == LONG_MIN) {
			ret = -ERANGE;
			pr_error("Cannot convert %s to fd\n", ent->d_name);
			free(fd);
			break;
		}

		INIT_LIST_HEAD(&fd->link);
		list_add_tail(&fd->link, head);
	}

	if (ret < 0) {
		pr_error("Failed to readdir %s: %s\n",
			 opened_fd_path, strerror(ret));
	}

	closedir(dir);
	return ret;
}

/* If cid is less than 0, it means real_fd is not in cfs */
int map_fd(struct client_info *ci, struct queue_info *queue_array[QUEUE_TYPE_NR],
	   int real_fd, int expected_fd, int64_t cid)
{
	struct fd_map_set *fds;
	bool found = false;
	int ret = -EINVAL;

	if (cid > 0) {
		assert(queue_array != NULL &&
		       queue_array[CTRL_QUEUE] != NULL &&
		       queue_array[DATA_QUEUE] != NULL &&
		       queue_array[DONE_QUEUE] != NULL);
	}

	pthread_rwlock_wrlock(&ci->rwlock);
	if (expected_fd == -1) {
		/* get the first free fd */
		list_for_each_entry(fds, &ci->fd_map_set_list, fds_link) {
			if (fds->free_nr > 0) {
				found = true;
				break;
			}
		}

		if (!found) {
			fds = do_append_fd_map_set(ci, AUTO_SELECT);
			if (fds == NULL) {
				pr_error("Failed to append fd_map_set\n");
				pthread_rwlock_unlock(&ci->rwlock);
				return ret;
			}

			fds->fd_maps[0].real_fd = real_fd;
			fds->fd_maps[0].cid = cid;
			for (int type = 0; type < QUEUE_TYPE_NR; type++)
				fds->fd_maps[0].queue_array[type] = cid < 0 ? NULL : queue_array[type];
			fds->free_nr--;
			ret = fds->start_fd;
		} else {
			for (int i = 0; i < FD_PER_SET; i++) {
				if (fds->fd_maps[i].real_fd == -1) {
					fds->fd_maps[i].real_fd = real_fd;
					fds->fd_maps[i].cid = cid;
					for (int type = 0; type < QUEUE_TYPE_NR; type++)
						fds->fd_maps[i].queue_array[type] = cid < 0 ? NULL : queue_array[type];
					fds->free_nr--;
					ret = fds->start_fd + i;
					break;
				}
			}
		}
	} else {
		/* must alloc expected fd */
		list_for_each_entry(fds, &ci->fd_map_set_list, fds_link) {
			if (fds->start_fd > expected_fd || fds->start_fd + FD_PER_SET <= expected_fd)
				continue;

			int offs = expected_fd - fds->start_fd;
			if (fds->fd_maps[offs].real_fd != -1) {
				pr_error("fd %d is already alloced\n", expected_fd);
				return -EINVAL;
			}

			fds->fd_maps[offs].real_fd = real_fd;
			fds->fd_maps[offs].cid = cid;
			for (int type = 0; type < QUEUE_TYPE_NR; type++)
				fds->fd_maps[offs].queue_array[type] = cid < 0 ? NULL : queue_array[type];
			fds->free_nr--;
			ret = expected_fd;

			break;
		}
		if (ret < 0) {
			/* expected fd has no corresponding fd_map_set, alloc one for it */
			fds = do_append_fd_map_set(ci, expected_fd);
			if (fds == NULL) {
				pr_error("Failed to append fd_map_set\n");
				pthread_rwlock_unlock(&ci->rwlock);
				return ret;
			}

			int offs = expected_fd - fds->start_fd;
			fds->fd_maps[offs].real_fd = real_fd;
			fds->fd_maps[offs].cid = cid;
			for (int type = 0; type < QUEUE_TYPE_NR; type++)
				fds->fd_maps[offs].queue_array[type] = cid < 0 ? NULL : queue_array[type];
			fds->free_nr--;
			ret = fds->start_fd;
		}
	}

	if (ret >= 0)
		pr_debug("map fd %d for real_fd %d expected_fd %d cid %"PRId64" from fds %d free_nr %d\n",
			 ret, real_fd, expected_fd, cid, fds->start_fd, fds->free_nr);

	pthread_rwlock_unlock(&ci->rwlock);

	return ret;
}

