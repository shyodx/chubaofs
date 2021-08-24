#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "client.h"
#include "fd_map.h"
#include "list.h"
#include "log.h"

/* only has one client for tasks */
struct client_info client = {0};

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

struct client_info *init_client(const char *fstype)
{
	size_t len = strlen(fstype) + 1;
	int err;

	client.fstype = malloc(len);
	if (client.fstype == NULL)
		return NULL;

	client.flags = 0;
	client.fd_map_set_nr = 0;
	client.total_free_fd_nr = 0;
	memcpy(client.fstype, fstype, len);

	err = pthread_rwlock_init(&client.rwlock, NULL);
	if (err != 0) {
		free(client.fstype);
		client.fstype = NULL;
		return NULL;
	}

	INIT_LIST_HEAD(&client.fd_map_set_list);
	INIT_LIST_HEAD(&client.mountpoint_list);
	INIT_LIST_HEAD(&client.client_link);

	return &client;
}

struct client_info *get_client(void)
{
	return &client;
}

void destroy_client(void)
{
	pthread_rwlock_wrlock(&client.rwlock);
	destroy_fd_map_set_nolock(&client);
	destroy_mountpoints_nolock(&client);
	free(client.fstype);
	client.fstype = NULL;
	pthread_rwlock_unlock(&client.rwlock);

	pr_debug("Free client\n");
}

int set_client_flag(struct client_info *ci, unsigned int flag)
{
	pthread_rwlock_wrlock(&ci->rwlock);
	ci->flags |= flag;
	pthread_rwlock_unlock(&ci->rwlock);

	pr_debug("Set client flags %#x\n", ci->flags);
}
