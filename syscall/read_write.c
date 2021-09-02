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
		pr_debug("fd %d real_fd %d offset %jd count %zu is in cfs path cid %"PRId64"\n",
			 fd, map.real_fd, map.offset, count, map.cid);
		ret = cfs_read(map.cid, map.real_fd, buf, count, map.offset);
	} else {
		pr_debug("fd %d real_fd %d is NOT in cfs path\n",
			 fd, map.real_fd);
		ret = orig_apis.read(map.real_fd, buf, count);
	}

	if (ret > 0) {
		map.offset += ret;
		update_fd(ci, fd, &map);
	}

out:
	put_client(ci);
	SET_ERRNO(ret);
	return ret < 0 ? -1 : ret;
}

ssize_t write(int fd, const void *buf, size_t count)
{
	struct client_info *ci = get_client(getpid());
	struct fd_map map = {0};
	ssize_t ret;

	pr_debug("client %"PRId64" write fd %d\n", ci->id, fd);

	ret = (ssize_t)query_fd(ci, fd, &map);
	if (ret < 0) {
		pr_error("Failed to get real fd of fd %d: %s\n", fd, strerror((int)(-ret)));
		goto out;
	}

	if (map.cid >= 0) {
		pr_debug("fd %d real_fd %d offset %jd count %zu is in cfs path cid %"PRId64"\n",
			 fd, map.real_fd, map.offset, count, map.cid);
		ret = cfs_write(map.cid, map.real_fd, (char *)buf, count, map.offset);
	} else {
		pr_debug("fd %d real_fd %d is NOT in cfs path\n",
			 fd, map.real_fd);
		ret = orig_apis.write(map.real_fd, buf, count);
	}

	if (ret > 0) {
		map.offset += ret;
		update_fd(ci, fd, &map);
	}

out:
	put_client(ci);
	SET_ERRNO(ret);
	return ret < 0 ? -1 : ret;
}
