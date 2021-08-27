#ifndef _CFS_APIS_H
#define _CFS_APIS_H

struct orig_apis {
	int(*open)(const char *pathname, int flags, ...);
	int(*close)(int fd);
};

extern struct orig_apis orig_apis;
#endif
