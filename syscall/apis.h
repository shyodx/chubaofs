#ifndef _CFS_APIS_H
#define _CFS_APIS_H

#include <sys/types.h>

struct orig_apis {
	int(*open)(const char *pathname, int flags, ...);
	int(*close)(int fd);
	ssize_t(*read)(int fd, void *buf, size_t count);
	ssize_t(*write)(int fd, const void *buf, size_t count);

	pid_t(*fork)(void);
};

extern struct orig_apis orig_apis;
#endif
