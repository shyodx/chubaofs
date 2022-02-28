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
#include <sys/types.h>

#include "common.h"
#include "client.h"
#include "apis.h"
#include "fd_map.h"
#include "log.h"

struct hold_file_params {
	int fd;
} __attribute__((packed));

static void cleanup_parent_mem(struct client_info *ci)
{
	struct mountpoint *mnt, *next;

	destroy_fd_map_set_nolock(ci);
	list_for_each_entry_safe(mnt, next, &ci->mountpoint_list, mountpoint_link) {
		list_del(&mnt->mountpoint_link);
		free(mnt->config.master_addr);
		free(mnt->config.volname);
		free(mnt->config.log_dir);
		free(mnt->config.log_level);
		free(mnt->config.follower_read);
		free(mnt);
	}
	free(ci);
	/* FIXME: need also cleanup shared memory */
}

static int cfs_hold_file(int64_t cid, int fd, struct fd_map *map)
{
	struct queue_info *ctrl = map->queue_array[CTRL_QUEUE];
	struct queue_info *done = map->queue_array[DONE_QUEUE];
	struct ctrl_item *ctrl_item = NULL;
	struct done_item *done_item = NULL;
	uint64_t req_id;
	struct queue_item_params item_params = {0};
	struct hold_file_params *hold_file_params;
	int ret;

	req_id = next_req_id();
	pr_debug("Get Req[%"PRIu64"]\n", req_id);

	ret = queue_get_items(map->queue_array, &item_params);
	if (ret < 0)
		return ret;

	ctrl_item = (struct ctrl_item *)queue_item(ctrl, item_params.ctrl_idx);
	assert(ctrl_item->state == CTRL_STATE_NEW);
	ctrl_item->req_id = req_id;
	ctrl_item->opcode = CBFS_HOLD_FILE;
	ctrl_item->done_idx = item_params.done_idx;
	pr_debug("Req[%"PRIu64"] Get ctrl idx %u\n", req_id, item_params.ctrl_idx);

	done_item = (struct done_item *)queue_item(done, item_params.done_idx);
	init_done_item(done_item, req_id, CBFS_HOLD_FILE);
	pr_debug("Req[%"PRIu64"] Get done idx %u\n", req_id, item_params.done_idx);

	hold_file_params = (struct hold_file_params *)ctrl_item->data;
	hold_file_params->fd = map->real_fd;
	pr_debug("DEBUG: hold_file_params %p fd %d\n", hold_file_params, hold_file_params->fd);

	/* update ctrl state at the end of preparation */
	queue_mark_item_ready(ctrl_item);
	pr_debug("Req[%"PRIu64"] Mark ctrl idx %u ready\n", req_id, item_params.ctrl_idx);

	/* wait for done */
	ret = queue_poll_item(ctrl, done_item);
	pr_debug("Req[%"PRIu64"] ctrl idx %u done idx %u get return %d\n",
		 req_id, item_params.ctrl_idx, item_params.done_idx, ret);

	queue_put_items(map->queue_array, ctrl_item);

	return ret;
}

pid_t fork(void)
{
	struct client_info *parent_ci = get_client(getpid());
	struct client_info *child_ci;
	pid_t pid;
	int ret;

	pr_debug("==> start fork parent pid %d\n", getpid());
	/*
	 * lock client_info to avoid any update during fork, so that child
	 * could clone client_info safely
	 */
	pthread_rwlock_wrlock(&parent_ci->rwlock);
	pid = orig_apis.fork();
	if (pid < 0) {
		pthread_rwlock_unlock(&parent_ci->rwlock);
		put_client(parent_ci);
		pr_error("Failed to fork: %d\n", errno);
		return pid;
	}

	if (pid > 0) {
		pthread_rwlock_unlock(&parent_ci->rwlock);
		put_client(parent_ci);
		pr_debug("Parent get child pid %d\n", pid);
		return pid;
	}

	/* child */
	pid = getpid();
	pr_debug("==> child %d start\n", pid);

	child_ci = clone_client(parent_ci);
	if (child_ci == NULL) {
		exit(-ENOMEM);
	}

	/* cleanup memory allocated by parent? */
	cleanup_parent_mem(parent_ci);

	ret = register_client(child_ci);
	if (ret < 0) {
		pr_error("Failed to register client: %s\n", strerror(ret));
		exit(ret);
	}

	/* update fd map */
	struct fd_map_set *fds;
	/* FIXME: cannot handle if we have several mountpoints */
	struct mountpoint *mnt = list_first_entry(&child_ci->mountpoint_list, struct mountpoint, mountpoint_link);
	list_for_each_entry(fds, &child_ci->fd_map_set_list, fds_link) {
		for (int i = 0; i < FD_PER_SET; i++) {
			/* skip unused slots */
			if (fds->fd_maps[i].real_fd == -1)
				continue;
			/* skip non-cfs slots */
			if (fds->fd_maps[i].cid == -1)
				continue;
			fds->fd_maps[i].cid = child_ci->appid;
			for (int type = CTRL_QUEUE; type < QUEUE_TYPE_NR; type++) {
				pr_debug("Set queue_arrary[%d] %p for fd %d:%d\n",
					 type, mnt->queue_array[type], fds->start_fd + i, fds->fd_maps[i].real_fd);
				fds->fd_maps[i].queue_array[type] = mnt->queue_array[type];
			}
			// FIXME: open to increase refcnt
			cfs_hold_file(child_ci->appid, fds->fd_maps[i].real_fd, &fds->fd_maps[i]);
		}
	}

	gci = child_ci;
	return 0;
}

pid_t vfork(void)
{
	return -ENOSYS;
}

int clone(int (*fn)(void *), void *stack, int flags, void *arg, ...)
{
	return -ENOSYS;
}
