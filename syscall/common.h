#ifndef _CFS_SYSCALL_COMMON_H
#define _CFS_SYSCALL_COMMON_H

#define PATH_MAX 4096

#define dot "."
#define dotdot ".."

#define SET_ERRNO(ret) do { errno = (ret) < 0 ? -ret : 0; } while (0)

#endif
