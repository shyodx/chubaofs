#ifndef _CFS_SYSCALL_FD_MAP_H
#define _CFS_SYSCALL_FD_MAP_H

#include <pthread.h>
#include <stdbool.h>

#include "list.h"
#include "client.h"

#define FD_PER_SET 64

struct fd_map {
	int real_fd;
	bool is_cfs_fd;
};

struct fd_map_set {
	struct fd_map fd_maps[FD_PER_SET];
	int start_fd;
	unsigned int free_nr;
	struct list_head fds_link;
};

struct open_fd {
	int fd;
	struct list_head link;
};

int append_fd_map_set(struct client_info *ci);
int map_fd(struct client_info *ci, int real_fd, int expected_fd, bool is_cfs_fd);
int get_opened_fd(struct client_info *ci, struct list_head *head);
void destroy_fd_map_set_nolock(struct client_info *ci);

#endif
