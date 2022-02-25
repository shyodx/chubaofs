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

#ifndef _CFS_PRELOAD_LIB_LOG_H
#define _CFS_PRELOAD_LIB_LOG_H

#include <stdio.h>

#define pr_error(fmt, ...) do {								\
	fprintf(stderr, "[ERROR][%s:%d] " fmt, __func__, __LINE__, ##__VA_ARGS__);	\
} while (0)

#define pr_warn(fmt, ...) do {								\
	fprintf(stderr, "[WARN][%s:%d] " fmt, __func__, __LINE__, ##__VA_ARGS__);	\
} while (0)

#define pr_info(fmt, ...) do {								\
	fprintf(stdout, "[INFO][%s:%d] " fmt, __func__, __LINE__, ##__VA_ARGS__);	\
} while (0)

#define pr_msg(fmt, ...) do {								\
	fprintf(stdout, fmt, ##__VA_ARGS__);						\
} while (0)

#define DEBUG_ENABLED
#ifdef DEBUG_ENABLED
#define pr_debug(fmt, ...) do {								\
	fprintf(stdout, "[DEBUG][%s:%d] " fmt, __func__, __LINE__, ##__VA_ARGS__);	\
} while (0)
#else
#define pr_debug(fmt, ...)
#endif

#endif
