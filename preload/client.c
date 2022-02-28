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
#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <errno.h>

#include "client.h"
#include "fd_map.h"
#include "list.h"
#include "log.h"

/* FIXME: could get cfs_config info from env */
static int init_cfs_config(struct mountpoint *mnt, const char *volname)
{
	mnt->config.master_addr = strdup("192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010");
	mnt->config.volname = (char *)volname;
	mnt->config.log_dir = strdup("/tmp/cfs");
	mnt->config.log_level = strdup("debug");
	mnt->config.follower_read = strdup("true");

	pr_debug("init cfs_config:\n\tmasterAddr: %s\n\tvolName: %s\n\t"
		 "logDir: %s\n\tlogLevel: %s\n\tfollowerRead: %s\n",
		 mnt->config.master_addr, mnt->config.volname,
		 mnt->config.log_dir, mnt->config.log_level,
		 mnt->config.follower_read);

	return 0;
}

/* FIXME: only for linux */
int append_mountpoint(struct client_info *ci, const char *mnt_fsname, const char *mnt_dir)
{
	struct mountpoint *mnt;
	struct fsname_dir *fsname_dir;
	char *volname;
	size_t len;
	int err;

	/* init mountpoint's mnt_dir */
	len = strlen(mnt_dir) + 1 + strlen(mnt_fsname) + 1;
	mnt = malloc(sizeof(struct mountpoint) + len);
	if (mnt == NULL) {
		return -ENOMEM;
	}
	memset(mnt, 0, sizeof(struct mountpoint));

	fsname_dir = &mnt->fsname_dir;
	fsname_dir->offs = (int)strlen(mnt_dir) + 1;
	memcpy(MNT_DIR(mnt), mnt_dir, fsname_dir->offs);
	memcpy(MNT_FSNAME(mnt), mnt_fsname, (int)len - fsname_dir->offs);
	INIT_LIST_HEAD(&mnt->mountpoint_link);

	/* get volume name from mnt_fsname */
	if (strlen(mnt_fsname) < strlen("chubaofs-")) {
		free(mnt);
		pr_error("Invalid fsname '%s'\n", mnt_fsname);
		return -EINVAL;
	}
	volname = strdup(mnt_fsname + strlen("chubaofs-"));
	if (volname == NULL) {
		free(mnt);
		return -ENOMEM;
	}

	/* get volnume info and init cfs_config */
	err = init_cfs_config(mnt, volname);
	if (err < 0) {
		free(volname);
		free(mnt);
		return err;
	}

	/* insert mountpoint to client's mountpoint_list */
	pthread_rwlock_wrlock(&ci->rwlock);
	list_add(&mnt->mountpoint_link, &ci->mountpoint_list);
	pthread_rwlock_unlock(&ci->rwlock);

	pr_debug("Add cfs mountpoint volume '%s' mnt_dir '%s'\n", volname, mnt_dir);

	return 0;
}

static void destroy_mountpoints_nolock(struct client_info *ci)
{
	struct mountpoint *mnt, *next;

	if (ci == NULL)
		return;

	list_for_each_entry_safe(mnt, next, &ci->mountpoint_list, mountpoint_link) {
		pr_debug("free mountpoint %s client %"PRId64"\n", MNT_DIR(mnt), mnt->cid);
		int refcnt = atomic_load(&mnt->refcnt);
		if (refcnt != 0)
			pr_warn("mountpoint %s client %"PRId64" refcnt %d is still busy\n",
				MNT_DIR(mnt), mnt->cid, refcnt);
		list_del(&mnt->mountpoint_link);
		// FIXME: need tell fuse client to munmap
		//cfs_close_client(mnt->cid);
		free(mnt->config.master_addr);
		free(mnt->config.volname);
		free(mnt->config.log_dir);
		free(mnt->config.log_level);
		free(mnt->config.follower_read);
		free(mnt);
	}
}

struct client_info *alloc_client(const char *fstype, pid_t pid)
{
	struct client_info *ci;
	size_t len = strlen(fstype) + 1;
	int err;

	ci = malloc(sizeof(struct client_info) + len);
	if (ci == NULL)
		return NULL;

	ci->appid = 0;
	ci->pid = pid;
	ci->flags = CI_FLAG_NEW;
	ci->fd_map_set_nr = 0;
	ci->total_free_fd_nr = 0;
	atomic_init(&ci->refcnt, 1);
	memcpy(ci->fstype, fstype, len);

	err = pthread_rwlock_init(&ci->rwlock, NULL);
	if (err != 0) {
		goto free_out;
	}

	INIT_LIST_HEAD(&ci->fd_map_set_list);
	INIT_LIST_HEAD(&ci->mountpoint_list);
	//INIT_LIST_HEAD(&ci->client_link);

	return ci;

free_out:
	free(ci);
	return NULL;
}

int unregister_client(struct client_info *ci);

void destroy_client(struct client_info *ci)
{
	if (ci == NULL)
		return;

	/*
	 * It's safe to call list_del multiple times. destroy_client() could
	 * be called alone, so we should keep list_del here.
	 */
	//pthread_rwlock_wrlock(&client_list_lock);
	//list_del_init(&ci->client_link);
	//pthread_rwlock_unlock(&client_list_lock);

	pthread_rwlock_wrlock(&ci->rwlock);
	destroy_fd_map_set_nolock(ci);
	unregister_client(ci);
	destroy_mountpoints_nolock(ci);
	pthread_rwlock_unlock(&ci->rwlock);

	pr_debug("Free client %"PRId64"\n", ci->appid);
	free(ci);
}

void destroy_all_clients(void)
{
	destroy_client(gci);
	//struct client_info *ci, *next;

	//pthread_rwlock_wrlock(&client_list_lock);
	//while (!list_empty(&client_list)) {
	//	ci = list_first_entry(&client_list, struct client_info, client_link);
	//	list_del_init(&ci->client_link);
	//	pthread_rwlock_unlock(&client_list_lock);

	//	destroy_client(ci);

	//	pthread_rwlock_wrlock(&client_list_lock);
	//}
	//pthread_rwlock_unlock(&client_list_lock);
}

int unregister_client(struct client_info *ci)
{
	struct mountpoint *mnt;
	int sockfd;
	int err;

	pthread_rwlock_wrlock(&ci->rwlock);
	list_for_each_entry(mnt, &ci->mountpoint_list, mountpoint_link) {
		sockfd = connect_to_daemon(MNT_FSNAME(mnt));
		if (sockfd < 0) {
			err = sockfd;
			break;
		}

		err = queue_unregister(sockfd, ci->appid);
		if (err < 0) {
			pr_error("Unregister client %"PRId64" on mnt %s fail: %d\n",
				 ci->appid, MNT_FSNAME(mnt), err);
			disconnect_to_daemon(sockfd);
			break;
		}

		disconnect_to_daemon(sockfd);

		for (int type = CTRL_QUEUE; type < QUEUE_TYPE_NR; type++)
			queue_destroy(mnt->queue_array[type]);
	}
	pthread_rwlock_unlock(&ci->rwlock);

	if (err < 0) {
		pr_error("Failed to unregister client: %d\n", err);
		return err;
	}

	pr_debug("Unregister client %"PRId64"\n", ci->appid);
	return 0;
}

int register_client(struct client_info *ci)
{
	struct mountpoint *mnt;
	int sockfd;
	int err;

	pr_debug("==> start register client\n");

	pthread_rwlock_wrlock(&ci->rwlock);
	if (ci->flags != CI_FLAG_NEW && (ci->flags & CI_FLAG_CLONE) == 0) {
		pthread_rwlock_unlock(&ci->rwlock);
		pr_error("Invalid client flags %x\n", ci->flags);
		return -EINVAL;
	}

	list_for_each_entry(mnt, &ci->mountpoint_list, mountpoint_link) {
		for (int type = CTRL_QUEUE; type < QUEUE_TYPE_NR; type++) {
			err = queue_create(type, CTRL_QUEUE_MEMBERS_ORDER, &mnt->queue_array[type]);
			if (err < 0) {
				break;
			}
		}

		sockfd = connect_to_daemon(MNT_FSNAME(mnt));
		if (sockfd < 0) {
			err = sockfd;
			break;
		}

		if (ci->flags == CI_FLAG_NEW) {
			pr_debug("Register new client\n");
			err = queue_register(sockfd, mnt->queue_array, (uint64_t *)&ci->appid);
		}
		if (err < 0) {
			disconnect_to_daemon(sockfd);
			break;
		}

		/* FIXME: saving cid in mnt is ugly, but how can we do connect_to_daemon()
		 * to wakeup daemon in queue_poll_item?
		 */
		mnt->cid = ci->appid;
		for (int type = CTRL_QUEUE; type < QUEUE_TYPE_NR; type++)
			mnt->queue_array[type]->mnt = mnt;

		disconnect_to_daemon(sockfd);
	}
	pthread_rwlock_unlock(&ci->rwlock);

	if (err < 0) {
		pr_error("Failed to register client: %d\n", err);
		return err;
	}

	//pthread_rwlock_wrlock(&client_list_lock);
	//list_add_tail(&ci->client_link, &client_list);
	//pthread_rwlock_unlock(&client_list_lock);

	pr_debug("Register client %"PRId64"\n", ci->appid);
	return 0;
}
