#ifndef _CFS_SYSCALL_CLIENT_H
#define _CFS_SYSCALL_CLIENT_H

#include <pthread.h>
#include <stdatomic.h>

#include "list.h"

extern struct list_head client_list;

#define CI_FLAG_NEW	0
#define CI_FLAG_READY	(1U << 0)

struct client_info {
	pthread_rwlock_t rwlock;
	pid_t pid;
	int64_t cid;
	atomic_int refcnt;
	unsigned int flags;
	unsigned int fd_map_set_nr;
	unsigned int total_free_fd_nr;
	struct list_head fd_map_set_list;
	struct list_head mountpoint_list;
	struct list_head client_link;

	char fstype[0]; /* KEEP IT AS THE LAST ELEMENT */
};

struct client_info *alloc_client(const char *fstype, pid_t pid);
void destroy_client(struct client_info *ci);
void destroy_all_clients(void);
int register_client(struct client_info *ci);
int append_mountpoint(struct client_info *ci, const char *mnt_dir);

#endif
