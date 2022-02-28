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

#ifndef _CFS_PRELOAD_LIB_FD_MAP_H
#define _CFS_PRELOAD_LIB_FD_MAP_H

#include <pthread.h>
#include <stdbool.h>

#include "list.h"
#include "client.h"
#include "queue.h"

#define FD_PER_SET 64

struct fd_map {
	int real_fd;
	off_t offset;
	int64_t cid;
	struct queue_info *queue_array[QUEUE_TYPE_NR];
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

#define IS_CFS_FD(map) ((map)->cid >= 0)

int append_fd_map_set(struct client_info *ci);
int map_fd(struct client_info *ci, struct queue_info *queue_array[QUEUE_TYPE_NR], int real_fd, int expected_fd, int64_t cid);
int unmap_fd(struct client_info *ci, int fd, struct fd_map *map);
int query_fd(struct client_info *ci, int fd, struct fd_map *map);
int update_fd(struct client_info *ci, int fd, struct fd_map *map);
int get_opened_fd(struct client_info *ci, struct list_head *head);
void destroy_fd_map_set_nolock(struct client_info *ci);

#endif
