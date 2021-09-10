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
	size_t len = strlen(mnt->mnt_dir);

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
		 ci->id, pathname, abs_path, flags, mode);

	mnt = get_mountpoint(ci, abs_path);
	if (mnt != NULL) {
		char *path_in_cfs = (char *)get_path_in_cfs(mnt, abs_path);
		real_fd = cfs_open(mnt->cid, path_in_cfs, flags, mode);
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

	fd = map_fd(ci, real_fd, -1, cid);
	if (fd < 0) {
		pr_error("Failed to map fd: %s\n", strerror(-fd));
		goto out_close;
	}

	if ((flags & O_APPEND) && (cid >= 0)) {
		/* get attr: file size */
		struct fd_map map;
		struct cfs_stat_info st;

		ret = cfs_fgetattr(cid, real_fd, &st);
		if (ret < 0) {
			goto out_close;
		}

		map.real_fd = real_fd;
		map.offset = (off_t)st.size;
		map.cid = cid;
		update_fd(ci, fd, &map);
	}

	ret = fd;
	goto out_put;

out_close:
	if (cid >= 0) {
		cfs_close(cid, real_fd);
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

int close(int fd)
{
	struct client_info *ci = get_client(getpid());
	struct fd_map map = {0};
	int real_fd;
	int ret;

	ret = unmap_fd(ci, fd, &map);
	if (ret < 0) {
		pr_error("Failed to unmap fd %d: %s\n", fd, strerror(-ret));
		/* Invalid fd? Let close return error */
		goto out;
	}

	if (IS_CFS_FD(&map)) {
		cfs_close(map.cid, map.real_fd);
	} else {
		ret = orig_apis.close(map.real_fd);
	}

out:
	put_client(ci);
	return ret;
}
