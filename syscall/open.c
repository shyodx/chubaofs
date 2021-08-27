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

int open(const char *pathname, int flags, ...)
{
	struct client_info *ci = get_client(getpid());
	mode_t mode = 0644; /* FIXME: the default value? */
	char *abs_path;
	int fd, real_fd;
	int64_t cid;
	int ret;

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

	cid = get_mountpoint_cid(ci, abs_path);
	if (cid >= 0) {
		pr_debug("file[%s] is in cfs path cid %"PRId64"\n", pathname, cid);
		real_fd = cfs_open(cid, (char *)pathname, flags, mode);
	} else {
		pr_debug("file[%s] is NOT in cfs path\n", pathname);
		real_fd = orig_apis.open(pathname, flags, mode);
	}

	free(abs_path);

	if (real_fd < 0) {
		ret = real_fd;
		goto out;
	}

	fd = map_fd(ci, real_fd, -1, cid);
	if (fd < 0) {
		pr_error("Failed to map fd: %s\n", strerror(-fd));
		if (cid >= 0) {
			cfs_close(cid, real_fd);
		} else {
			orig_apis.close(real_fd);
		}
	}
	ret = fd;

out:
	put_client(ci);
	return ret;
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
