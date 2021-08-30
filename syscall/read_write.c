#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdarg.h>
#include <errno.h>

#include "libcfs.h"

#include "common.h"
#include "client.h"
#include "fd_map.h"
#include "apis.h"
#include "log.h"

ssize_t read(int fd, void *buf, size_t count)
{
	struct client_info *ci = get_client(getpid());
	struct fd_map map = {0};
	ssize_t ret;

	pr_debug("client %"PRId64" read fd %d\n", ci->id, fd);

	ret = (ssize_t)query_fd(ci, fd, &map);
	if (ret < 0) {
		pr_error("Failed to get real fd of fd %d: %s\n", fd, strerror((int)(-ret)));
		goto out;
	}

	if (map.cid >= 0) {
		pr_debug("fd[%d] real_fd[%d] offset[%jd] is in cfs path cid %"PRId64"\n",
			 fd, map.real_fd, map.offset, map.cid);
		ret = cfs_read(map.cid, map.real_fd, buf, map.offset, count);
	} else {
		pr_debug("fd[%d] real_fd[%d] is NOT in cfs path\n",
			 fd, map.real_fd);
		ret = read(map.real_fd, buf, count);
	}

	if (ret > 0) {
		update_fd(ci, fd, &map);
	}

out:
	put_client(ci);
	return ret;
}
