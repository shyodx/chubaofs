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

#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdarg.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>

#include "common.h"
#include "client.h"
#include "fd_map.h"
#include "apis.h"
#include "log.h"

#define SLASH "/"

#define POP(stack, top, ret) do {	\
	if ((top) > 0)			\
		(top)--;		\
	if ((ret) != NULL)		\
		*(ret) = (stack)[top];	\
	(stack)[top] = NULL;		\
} while (0)

#define PUSH(stack, top, val) do {	\
	(stack)[top] = (val);		\
	(top)++;			\
} while (0)

struct close_params {
	int fd;
} __attribute__((packed));

static int cfs_close(int64_t cid, struct fd_map *map)
{
	struct queue_info *ctrl = map->queue_array[CTRL_QUEUE];
	struct queue_info *done = map->queue_array[DONE_QUEUE];
	struct ctrl_item *ctrl_item = NULL;
	struct done_item *done_item = NULL;
	uint64_t req_id;
	struct queue_item_params item_params = {0};
	struct close_params *close_params;
	int ret;

	req_id = next_req_id();
	pr_debug("Get Req[%"PRIu64"]\n", req_id);

	ret = queue_get_items(map->queue_array, &item_params);
	if (ret < 0)
		return ret;

	/* XXX: In fact, there should have no CBFS_CLOSE, but use flush and
	 * release (and others?) instead. Fortunately, daemon's cfs_close
	 * does these work for us, so we only need to call a CBFS_CLOSE
	 * simply.
	 */
	ctrl_item = (struct ctrl_item *)queue_item(ctrl, item_params.ctrl_idx);
	assert(ctrl_item->state == CTRL_STATE_NEW);
	ctrl_item->req_id = req_id;
	ctrl_item->opcode = CBFS_CLOSE;
	ctrl_item->done_idx = item_params.done_idx;
	pr_debug("Req[%"PRIu64"] Get ctrl idx %u\n", req_id, item_params.ctrl_idx);

	done_item = (struct done_item *)queue_item(done, item_params.done_idx);
	init_done_item(done_item, req_id, CBFS_CLOSE);
	pr_debug("Req[%"PRIu64"] Get done idx %u\n", req_id, item_params.done_idx);

	close_params = (struct close_params *)ctrl_item->data;
	close_params->fd = map->real_fd;
	pr_debug("DEBUG: close_params %p fd %d\n", close_params, close_params->fd);

	/* update ctrl state at the end of preparation */
	queue_mark_item_ready(ctrl_item);
	pr_debug("Req[%"PRIu64"] Mark ctrl idx %u ready\n", req_id, item_params.ctrl_idx);

	/* wait for done */
	ret = queue_poll_item(ctrl, done_item);
	pr_debug("Req[%"PRIu64"] ctrl idx %u done idx %u get return %d\n",
		 req_id, item_params.ctrl_idx, item_params.done_idx, ret);

	queue_put_items(map->queue_array, ctrl_item);

	return ret;
}

int close(int fd)
{
	struct client_info *ci = get_client(getpid());
	struct fd_map map = {0};
	int ret;

	ret = unmap_fd(ci, fd, &map);
	if (ret < 0) {
		pr_error("Failed to unmap fd %d: %s\n", fd, strerror(-ret));
		/* Invalid fd? Let close return error */
		goto out;
	}

	pr_debug("client %"PRId64" close fd[%d] real_fd[%d]\n",
		 ci->appid, fd, map.real_fd);

	if (IS_CFS_FD(&map)) {
		cfs_close(map.cid, &map);
	} else {
		ret = orig_apis.close(map.real_fd);
	}

out:
	put_client(ci);
	return ret;
}

struct open_params {
	uint32_t flags;
	uint32_t mode;
	uint16_t pathlen;
	uint16_t rsvd1;
	uint32_t rsvd2;
	char path[0];
} __attribute__((packed));

static int cfs_open(struct mountpoint *mnt, const char *path,
		    int flags, mode_t mode)
{
	int fd;
	struct queue_info *ctrl = mnt->queue_array[CTRL_QUEUE];
	struct queue_info *data = mnt->queue_array[DATA_QUEUE];
	struct queue_info *done = mnt->queue_array[DONE_QUEUE];
	struct ctrl_item *ctrl_item = NULL;
	struct data_item *data_item = NULL;
	struct done_item *done_item = NULL;
	uint64_t req_id;
	struct open_params *open_params;
	size_t params_size = sizeof(struct open_params) + strlen(path);
	struct queue_item_params item_params = {0};
	int ret;

	req_id = next_req_id();
	pr_debug("Get Req[%"PRIu64"]\n", req_id);

	if (params_size > CTRL_DATA_SIZE) {
		if (strlen(path) > DATA_DATA_SIZE) {
			pr_error("path length %zu too long, not supported for now\n",
				 strlen(path));
			return -ENAMETOOLONG;
		}
		item_params.data_expected = 1;
	}

	ret = queue_get_items(mnt->queue_array, &item_params);
	if (ret < 0)
		return ret;

	/* FIXME: how to restore alloced ctrl item if fail, add state to mark it as invalidate? */
	ctrl_item = (struct ctrl_item *)queue_item(ctrl, item_params.ctrl_idx);
	assert(ctrl_item->state == CTRL_STATE_NEW);
	ctrl_item->req_id = req_id;
	ctrl_item->opcode = CBFS_OPEN;
	ctrl_item->data_idx = item_params.data_idx;
	ctrl_item->done_idx = item_params.done_idx;
	pr_debug("Req[%"PRIu64"] Get ctrl idx %u\n", req_id, item_params.ctrl_idx);

	done_item = (struct done_item *)queue_item(done, item_params.done_idx);
	init_done_item(done_item, req_id, CBFS_OPEN);
	pr_debug("Req[%"PRIu64"] Get done idx %u\n", req_id, item_params.done_idx);

	open_params = (struct open_params *)ctrl_item->data;
	open_params->flags = (uint32_t)flags;
	open_params->mode = (uint32_t)mode;
	open_params->pathlen = (uint16_t)strlen(path);

	if (item_params.data_expected > 0) {
		data_item = (struct data_item *)queue_item(data, item_params.data_idx);
		init_data_item(data_item, req_id, CBFS_OPEN/*, params_size*/);
		ctrl_item->state |= CTRL_STATE_DATA_ITEM;
		memcpy(data_item->data, path, strlen(path));
		pr_debug("Req[%"PRIu64"] Get data idx %u\n", req_id, item_params.data_idx);
	} else {
		ctrl_item->state |= CTRL_STATE_INLINE_DATA;
		memcpy(open_params->path, path, strlen(path));
		pr_debug("Req[%"PRIu64"] Set ctrl idx %u inline data\n", req_id, item_params.ctrl_idx);
	}

	pr_debug("DEBUG: open_params %p path %p\n", open_params, &open_params->path);
	unsigned char *ptr = (unsigned char *)open_params;
	for (int i = 0; i < sizeof(struct open_params) + open_params->pathlen; i++) {
		printf(" %d", *(ptr + i));
	}
	printf("\n");

	/* update ctrl state at the end of preparation */
	queue_mark_item_ready(ctrl_item);
	pr_debug("Req[%"PRIu64"] Mark ctrl idx %u ready\n", req_id, item_params.ctrl_idx);
	//ctrl_item_tostring(ctrl_item, buf);

	/* wait for done */
	fd = queue_poll_item(ctrl, done_item);
	pr_debug("Req[%"PRIu64"] ctrl idx %u done idx %u get return %d\n",
		 req_id, item_params.ctrl_idx, item_params.done_idx, fd);

	queue_put_items(mnt->queue_array, ctrl_item);

	return fd;
}

static char *canonical_path(const char *pathname)
{
	/* FIXME: is 256 enough? */
	char *tmp_path = strdup(pathname);
	char *path_comps[256] = {NULL};
	char *comp;
	char *ret_path;
	size_t path_len = 0;
	int top = 0;

	if (tmp_path == NULL)
		return NULL;

	comp = strtok(tmp_path, SLASH);
	while (comp != NULL) {
		if (!strcmp(comp, dot)) {
			/* ignore '.', do nothing */
		} else if (!strcmp(comp, dotdot)) {
			char *ret_val = NULL;
			/* go back to parent directory */
			POP(path_comps, top, &ret_val);
		} else {
			PUSH(path_comps, top, comp);
			pr_debug("push '%s'\n", comp);
		}
		comp = strtok(NULL, SLASH);
	}

	for (int i = 0; i < 256 && path_comps[i] != NULL; i++) {
		path_len += 1; /* '/' */
		path_len += strlen(path_comps[i]);
	}
	path_len += 1; /* tail '\0' */

	ret_path = malloc(path_len);
	if (ret_path == NULL) {
		free(tmp_path);
		return NULL;
	}

	comp = ret_path;
	for (int i = 0; i < 256 && path_comps[i] != NULL; i++) {
		*comp = '/';
		comp++;
		strncpy(comp, path_comps[i], strlen(path_comps[i]));
		comp += strlen(path_comps[i]);
	}
	*comp = '\0';

	free(tmp_path);

	return ret_path;
}

/* the retuan value should be freed */
/* FIXME: deal with . and .. */
static char *absolute_path(const char *pathname)
{
	char *cwd;
	size_t cwd_len;
	int err;

	if (pathname[0] == '/') {
		/* pathname already starts from root */
		return canonical_path(pathname);
	}

	cwd = malloc(PATH_MAX);
	if (cwd == NULL)
		return NULL;

	cwd = getcwd(cwd, PATH_MAX);
	if (cwd == NULL) {
		pr_error("Failed to getcwd: %s\n", strerror(errno));
		free(cwd);
		return NULL;
	}

	pr_debug("Get cwd %s\n", cwd);
	cwd_len = strlen(cwd);
	err = snprintf(cwd + cwd_len, PATH_MAX - cwd_len, "/%s", pathname);
	if (err < 0) {	       
		pr_error("Failed to concatenate cwd [%s] with pathname[%s]: %s\n",
			 cwd, pathname, strerror(errno));
		free(cwd);
		return NULL;
	}

	return canonical_path(cwd);
}

static inline bool is_valid_open_flags(int flags)
{
	if (flags & ~(O_CREAT | O_APPEND | O_TRUNC | O_RDWR | O_WRONLY | O_RDONLY))
		return false;

	return true;
}

static const char *get_path_in_cfs(const struct mountpoint *mnt, const char *fullpath)
{
	size_t len = strlen(MNT_DIR(mnt));

	return fullpath + len;
}

int open(const char *pathname, int flags, ...)
{
	struct client_info *ci = get_client(getpid());
	struct mountpoint *mnt = NULL;
	mode_t mode = 0644; /* FIXME: the default value? */
	char *abs_path;
	int fd, real_fd;
	int64_t cid;
	int ret;

	if (!is_valid_open_flags(flags)) {
		pr_error("Unsupported flags %#x\n", flags);
		ret = -EINVAL;
		goto out;
	}

	if (flags & O_CREAT) {
		va_list list;

		va_start(list, flags);
		mode = va_arg(list, mode_t);
		va_end(list);
	}

	abs_path = absolute_path(pathname);
	if (abs_path == NULL) {
		pr_error("Failed to get absolute path of '%s'\n", pathname);
		ret = -ERANGE;
		goto out;
	}

	pr_debug("client %"PRId64" open file[%s] abs_path[%s] flags %#x mode 0%o\n",
		 ci->appid, pathname, abs_path, flags, mode);

	mnt = get_mountpoint(ci, abs_path);
	if (mnt != NULL) {
		char *path_in_cfs = (char *)get_path_in_cfs(mnt, abs_path);
		real_fd = cfs_open(mnt, path_in_cfs, flags, mode);
		pr_debug("file[%s] is in cfs path[%s] cid %"PRId64" real_fd %d\n",
			 pathname, path_in_cfs, mnt->cid, real_fd);
		cid = mnt->cid;
	} else {
		real_fd = orig_apis.open(abs_path, flags, mode);
		pr_debug("file[%s] is NOT in cfs path real_fd %d\n", pathname, real_fd);
		cid = -1;
	}

	free(abs_path);
	abs_path = NULL;

	if (real_fd < 0) {
		pr_error("Failed to open file[%s]: %d\n", pathname, real_fd);
		ret = real_fd;
		goto out_put;
	}

	/* FIXME: need make sure queue_array will not be freed, if open file
	 * still needs it. Maybe their refcnts should be increased in cfs_open,
	 * and decresed in cfs_close.
	 */
	fd = map_fd(ci, mnt->queue_array, real_fd, -1, cid);
	if (fd < 0) {
		pr_error("Failed to map fd: %s\n", strerror(-fd));
		goto out_close;
	}

	//if ((flags & O_APPEND) && (cid >= 0)) {
	//	/* get attr: file size */
	//	struct fd_map map;
	//	struct cfs_stat_info st;

	//	ret = cfs_fgetattr(cid, real_fd, &st);
	//	if (ret < 0) {
	//		goto out_close;
	//	}

	//	map.real_fd = real_fd;
	//	map.offset = (off_t)st.size;
	//	map.cid = cid;
	//	update_fd(ci, fd, &map);
	//}

	ret = fd;
	goto out_put;

out_close:
	if (cid >= 0) {
		struct fd_map map = {0};
		map.real_fd = real_fd;
		for (int i = 0; i < QUEUE_TYPE_NR; i++)
			map.queue_array[i] = mnt->queue_array[i];
		cfs_close(cid, &map);
	} else {
		orig_apis.close(real_fd);
	}
out_put:
	put_mountpoint(mnt);
out:
	put_client(ci);
	SET_ERRNO(ret);
	return ret < 0 ? -1 : ret;
}

/*
 * No need to get the original shm_open and shm_unlink, since they are
 * based on open and unlink
 */
int shm_open(const char *name, int flag, mode_t mode)
{
	char buf[NAME_MAX];

	/* FIXME: need check name */
	snprintf(buf, NAME_MAX, "/dev/shm/%s", name);
	flag |= (O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
	pr_debug("open shm file %s\n", buf);
	return orig_apis.open(buf, flag, mode);
}

int shm_unlink(const char *name)
{
	char buf[NAME_MAX];

	/* FIXME: need check name */
	snprintf(buf, NAME_MAX, "/dev/shm/%s", name);
	pr_debug("unlink shm file %s\n", buf);
	return orig_apis.unlink(buf);
}
