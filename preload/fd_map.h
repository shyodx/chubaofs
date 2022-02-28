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

#define FD_PER_SET 64

struct fd_map {
	int real_fd;
	off_t offset;
	int64_t cid;
};

struct fd_map_set {
	struct fd_map fd_maps[FD_PER_SET];
	int start_fd;
	unsigned int free_nr;
	struct list_head fds_link;
};

#define IS_CFS_FD(map) ((map)->cid >= 0)

#endif
