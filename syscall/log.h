#ifndef _CFS_SYSCALL_LOG_H
#define _CFS_SYSCALL_LOG_H

#include <stdio.h>

#define pr_error(fmt, ...) do {								\
	fprintf(stderr, "ERROR: %s:%d:" fmt, __func__, __LINE__, ##__VA_ARGS__);	\
} while (0)

#define pr_warn(fmt, ...) do {								\
	fprintf(stderr, "WARN: %s:%d:" fmt, __func__, __LINE__, ##__VA_ARGS__);		\
} while (0)

#define pr_info(fmt, ...) do {								\
	fprintf(stdout, "INFO: %s:%d:" fmt, __func__, __LINE__, ##__VA_ARGS__);		\
} while (0)

#define pr_msg(fmt, ...) do {								\
	fprintf(stdout, fmt, ##__VA_ARGS__);						\
} while (0)

#define DEBUG_ENABLED
#ifdef DEBUG_ENABLED
#define pr_debug(fmt, ...) do {								\
	fprintf(stdout, "DEBUG: %s:%d:" fmt, __func__, __LINE__, ##__VA_ARGS__);	\
} while (0)
#else
#define pr_debug(fmt, ...)
#endif

#endif
