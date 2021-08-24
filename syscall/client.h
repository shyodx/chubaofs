#ifndef _CFS_SYSCALL_CLIENT_H
#define _CFS_SYSCALL_CLIENT_H

#include <pthread.h>

#include "list.h"

#define CI_FLAG_NEW	0
#define CI_FLAG_READY	(1U << 0)

struct client_info {
	pthread_rwlock_t rwlock;
	char *fstype;
	unsigned int flags;
	unsigned int fd_map_set_nr;
	unsigned int total_free_fd_nr;
	struct list_head fd_map_set_list;
	struct list_head mountpoint_list;
	struct list_head client_link;

};

struct client_info *init_client(const char *fstype);
void destroy_client(void);
int set_client_flag(struct client_info *ci, unsigned int flag);
int append_mountpoint(struct client_info *ci, const char *mnt_dir);
struct client_info *get_client(void);

#endif
