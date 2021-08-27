#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <assert.h>
#include <inttypes.h>
#include <errno.h>

#include "libcfs.h"

#include "client.h"
#include "fd_map.h"
#include "list.h"
#include "log.h"

LIST_HEAD(client_list);
pthread_rwlock_t client_list_lock;
int64_t client_id = 1;

struct cfs_config {
	char *master_addr;
	char *volname;
	char *log_dir;
	char *log_level;
	char *follower_read;
};

/* Each mountpoint could have one cfs_client */
/* FIXME: we should provide an ioctl to get server info, so that we could
 * create an cfs client and config libsdk */
struct mountpoint {
	int64_t cid;
	struct cfs_config config;
	struct list_head mountpoint_link;
	char mnt_dir[0];
};

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
	char *volname;
	size_t len;
	int err;

	/* init mountpoint's mnt_dir */
	len = strlen(mnt_dir) + 1;
	mnt = malloc(sizeof(struct mountpoint) + len);
	if (mnt == NULL) {
		return -ENOMEM;
	}

	INIT_LIST_HEAD(&mnt->mountpoint_link);
	memcpy(mnt->mnt_dir, mnt_dir, len);

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
		pr_debug("free mountpoint %s client %"PRId64"\n", mnt->mnt_dir, mnt->cid);
		list_del(&mnt->mountpoint_link);
		cfs_close_client(mnt->cid);
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

	ci->id = 0;
	ci->pid = pid;
	ci->flags = 0;
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
	INIT_LIST_HEAD(&ci->client_link);

	return ci;

free_out:
	free(ci);
	return NULL;
}

struct client_info *get_client(pid_t pid)
{
	struct client_info *ci;
	bool found = false;

	pthread_rwlock_rdlock(&client_list_lock);
	list_for_each_entry(ci, &client_list, client_link) {
		if (ci->pid != pid)
			continue;

		atomic_fetch_add(&ci->refcnt, 1);
		found = true;
		break;
	}
	pthread_rwlock_unlock(&client_list_lock);

	if (!found) {
		pr_error("pid %d is not in client list!\n", pid);
		assert(false);
	}

	return ci;
}

void put_client(struct client_info *ci)
{
	int refcnt;

	if (ci == NULL)
		return;

	refcnt = atomic_fetch_sub(&ci->refcnt, 1);
	/* how to deal with refcnt that decreases to 0 */
}

void destroy_client(struct client_info *ci)
{
	if (ci == NULL)
		return;

	/*
	 * It's safe to call list_del multiple times. destroy_client() could
	 * be called alone, so we should keep list_del here.
	 */
	pthread_rwlock_wrlock(&client_list_lock);
	list_del_init(&ci->client_link);
	pthread_rwlock_unlock(&client_list_lock);

	pthread_rwlock_wrlock(&ci->rwlock);
	destroy_fd_map_set_nolock(ci);
	destroy_mountpoints_nolock(ci);
	pthread_rwlock_unlock(&ci->rwlock);

	pr_debug("Free client %"PRId64"\n", ci->id);
	free(ci);
}

void destroy_all_clients(void)
{
	struct client_info *ci, *next;

	pthread_rwlock_wrlock(&client_list_lock);
	while (!list_empty(&client_list)) {
		ci = list_first_entry(&client_list, struct client_info, client_link);
		list_del_init(&ci->client_link);
		pthread_rwlock_unlock(&client_list_lock);

		destroy_client(ci);

		pthread_rwlock_wrlock(&client_list_lock);
	}
	pthread_rwlock_unlock(&client_list_lock);
}

int register_client(struct client_info *ci)
{
	struct mountpoint *mnt;
	int err;

	pthread_rwlock_wrlock(&ci->rwlock);
	list_for_each_entry(mnt, &ci->mountpoint_list, mountpoint_link) {
		mnt->cid = cfs_new_client(); // never fail
		err = cfs_set_client(mnt->cid, "volName", mnt->config.volname);
		if (err < 0) {
			pr_error("Failed to set volName '%s' for client %"PRId64"\n",
				 mnt->config.volname, mnt->cid);
			break;
		}
		err = cfs_set_client(mnt->cid, "masterAddr", mnt->config.master_addr);
		if (err < 0) {
			pr_error("Failed to set masterAddr '%s' for client %"PRId64"\n",
				 mnt->config.master_addr, mnt->cid);
			break;
		}
		err = cfs_set_client(mnt->cid, "followerRead", mnt->config.follower_read);
		if (err < 0) {
			pr_error("Failed to set followerRead %s for client %"PRId64"\n",
				 mnt->config.follower_read, mnt->cid);
			break;
		}
		err = cfs_set_client(mnt->cid, "logDir", mnt->config.log_dir);
		if (err < 0) {
			pr_error("Failed to set logDir '%s' for client %"PRId64"\n",
				 mnt->config.log_dir, mnt->cid);
			break;
		}
		err = cfs_set_client(mnt->cid, "logLevel", mnt->config.log_level);
		if (err < 0) {
			pr_error("Failed to set logLevel '%s' for client %"PRId64"\n",
				 mnt->config.log_level, mnt->cid);
			break;
		}

		err = cfs_start_client(mnt->cid);
		if (err < 0) {
			pr_error("Failed to start client %"PRId64": %s\n", mnt->cid, strerror(err));
			break;
		}
	}
	pthread_rwlock_unlock(&ci->rwlock);

	pthread_rwlock_wrlock(&client_list_lock);
	ci->id = client_id++;
	list_add_tail(&ci->client_link, &client_list);
	pthread_rwlock_unlock(&client_list_lock);

	pr_debug("Register client %"PRId64"\n", ci->id);
}

int64_t get_mountpoint_cid(struct client_info *ci, const char *path)
{
	struct mountpoint *mnt;
	size_t path_len = strlen(path);
	size_t mnt_len;
	int64_t cid = -1;

	pthread_rwlock_rdlock(&ci->rwlock);
	list_for_each_entry(mnt, &ci->mountpoint_list, mountpoint_link) {
		mnt_len = strlen(mnt->mnt_dir);
		/* if we are accessing the root of cfs, mnt_len == path_len */
		if (mnt_len > strlen(path))
			continue;
		if (!memcmp(mnt->mnt_dir, path, mnt_len) &&
		    (path[mnt_len] == '\0' || path[mnt_len] == '/')) {
			cid = mnt->cid;
			break;
		}
	}
	pthread_rwlock_unlock(&ci->rwlock);

	return cid;
}
