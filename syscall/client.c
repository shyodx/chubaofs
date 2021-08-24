#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "client.h"
#include "fd_map.h"
#include "list.h"
#include "log.h"

LIST_HEAD(client_list);
pthread_rwlock_t client_list_lock;
int64_t client_id = 1;

struct mountpoint {
	struct list_head mountpoint_link;
	char mnt_dir[0];
};

int append_mountpoint(struct client_info *ci, const char *mnt_dir)
{
	struct mountpoint *mp;
	size_t len;
	int err;

	len = strlen(mnt_dir) + 1;
	mp = malloc(sizeof(struct mountpoint) + len);
	if (mp == NULL) {
		return -ENOMEM;
	}

	INIT_LIST_HEAD(&mp->mountpoint_link);
	memcpy(mp->mnt_dir, mnt_dir, len);

	pthread_rwlock_wrlock(&ci->rwlock);
	list_add(&mp->mountpoint_link, &ci->mountpoint_list);
	pthread_rwlock_unlock(&ci->rwlock);

	return 0;
}

static void destroy_mountpoints_nolock(struct client_info *ci)
{
	struct mountpoint *mnt, *next;

	if (ci == NULL)
		return;

	list_for_each_entry_safe(mnt, next, &ci->mountpoint_list, mountpoint_link) {
		pr_debug("free mountpoint %s\n", mnt->mnt_dir);
		list_del(&mnt->mountpoint_link);
		free(mnt);
	}
}

struct client_info *alloc_client(const char *fstype)
{
	struct client_info *ci;
	size_t len = strlen(fstype) + 1;
	int err;

	ci = malloc(sizeof(struct client_info) + len);
	if (ci == NULL)
		return NULL;

	ci->cid = 0;
	ci->flags = 0;
	ci->fd_map_set_nr = 0;
	ci->total_free_fd_nr = 0;
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

	pr_debug("Free client %ld\n", (long)ci->cid);
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
	pthread_rwlock_wrlock(&client_list_lock);
	ci->cid = client_id++;
	list_add_tail(&ci->client_link, &client_list);
	pthread_rwlock_unlock(&client_list_lock);

	pr_debug("Register client %ld\n", (long)ci->cid);
}
