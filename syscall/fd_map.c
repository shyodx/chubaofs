#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>

#include "fd_map.h"
#include "log.h"

#define FILE_PATH_LEN 128
#define dot "."
#define dotdot ".."

static struct fd_map_set *do_append_fd_map_set(struct client_info *ci)
{

	struct fd_map_set *fds;

	fds = malloc(sizeof(struct fd_map_set));
	if (fds == NULL)
		return NULL;

	for (int i = 0; i < FD_PER_SET; i++) {
		fds->fd_maps[i].real_fd = -1;
	}

	INIT_LIST_HEAD(&fds->fds_link);

	/* sorted fds */
	list_add_tail(&fds->fds_link, &ci->fd_map_set_list);
	ci->fd_map_set_nr++;
	ci->total_free_fd_nr += FD_PER_SET;

	return fds;
}

int append_fd_map_set(struct client_info *ci)
{
	struct fd_map_set *fds;

	pthread_rwlock_wrlock(&ci->rwlock);
	fds = do_append_fd_map_set(ci);
	pthread_rwlock_unlock(&ci->rwlock);
	if (fds == NULL)
		return -1;

	return 0;
}

void destroy_fd_map_set_nolock(struct client_info *ci)
{
	struct fd_map_set *fds, *next;

	if (ci == NULL)
		return;

	list_for_each_entry_safe(fds, next, &ci->fd_map_set_list, fds_link) {
		pr_debug("free fds %d\n", fds->start_fd);
		list_del(&fds->fds_link);
		free(fds);
	}
}

int get_opened_fd(struct client_info *ci, struct list_head *head)
{
	pid_t pid = getpid();
	char opened_fd_path[FILE_PATH_LEN] = {0};
	DIR *dir;
	struct dirent *ent;
	int ret;

	ret = snprintf(opened_fd_path, FILE_PATH_LEN, "/proc/%d/fd", pid);
	if (ret < 0 || ret == FILE_PATH_LEN) {
		pr_error("opened_fd_path too long for pid %d\n", pid);
		return -ENAMETOOLONG;
	}

	dir = opendir(opened_fd_path);
	if (dir == NULL) {
		ret = -errno;
		pr_error("Failed to opendir %s: %s\n", opened_fd_path, strerror(errno));
		return ret;
	}

	errno = 0;
	while (1) {
		ent = readdir(dir);
		if (ent == NULL) {
			ret = -errno;
			break;
		}

		pr_debug("Get opened fd %s\n", ent->d_name);
		if (!strcmp(ent->d_name, dot) || !strcmp(ent->d_name, dotdot))
			continue;

		struct open_fd *fd = malloc(sizeof(struct open_fd));
		if (fd == NULL) {
			ret = -ENOMEM;
			break;
		}

		fd->fd = (int)strtol(ent->d_name, NULL, 0);
		if (errno != 0 || fd->fd == LONG_MAX || fd->fd == LONG_MIN) {
			ret = -ERANGE;
			pr_error("Cannot convert %s to fd\n", ent->d_name);
			free(fd);
			break;
		}

		INIT_LIST_HEAD(&fd->link);
		list_add_tail(&fd->link, head);
	}

	if (ret < 0) {
		pr_error("Failed to readdir %s: %s\n",
			 opened_fd_path, strerror(ret));
	}

	closedir(dir);
	return ret;
}

/* If cid is less than 0, it means real_fd is not in cfs */
int map_fd(struct client_info *ci, int real_fd, int expected_fd, int64_t cid)
{
	struct fd_map_set *fds;
	bool found = false;
	int ret = -EINVAL;

	pthread_rwlock_wrlock(&ci->rwlock);
	if (expected_fd == -1) {
		/* get the first free fd */
		list_for_each_entry(fds, &ci->fd_map_set_list, fds_link) {
			if (fds->free_nr > 0) {
				found = true;
				break;
			}
		}

		if (!found) {
			fds = do_append_fd_map_set(ci);
			if (fds == NULL) {
				pr_error("Failed to append fd_map_set\n");
				pthread_rwlock_unlock(&ci->rwlock);
				return ret;
			}

			fds->fd_maps[0].real_fd = real_fd;
			fds->fd_maps[0].cid = cid;
			ret = fds->start_fd;
		} else {
			for (int i = 0; i < FD_PER_SET; i++) {
				if (fds->fd_maps[i].real_fd == -1) {
					ret = fds->start_fd + i;
					break;
				}
			}
		}
	} else {
		/* must alloc expected fd */
		list_for_each_entry(fds, &ci->fd_map_set_list, fds_link) {
			if (fds->start_fd > expected_fd || fds->start_fd + FD_PER_SET <= expected_fd)
				continue;

			int offs = expected_fd - fds->start_fd;
			if (fds->fd_maps[offs].real_fd != -1) {
				pr_error("fd %d is already alloced\n", expected_fd);
				return -EINVAL;
			}

			fds->fd_maps[offs].real_fd = real_fd;
			fds->fd_maps[offs].cid = cid;
			ret = expected_fd;

			break;
		}
	}

	if (ret >= 0)
		pr_debug("map fd %d for real_fd %d expected_fd %d cid %"PRId64" from fds %d\n",
			 ret, real_fd, expected_fd, cid, fds->start_fd);

	pthread_rwlock_unlock(&ci->rwlock);

	return ret;
}

/* Return the real_fd of fd */
int unmap_fd(struct client_info *ci, int fd, struct fd_map *map)
{
	struct fd_map_set *fds;
	int real_fd;

	pthread_rwlock_wrlock(&ci->rwlock);
	list_for_each_entry(fds, &ci->fd_map_set_list, fds_link) {
		if (fds->start_fd > fd || fds->start_fd + FD_PER_SET <= fd)
			continue;

		int offs = fd - fds->start_fd;
		if (fds->fd_maps[offs].real_fd == -1) {
			pr_debug("fd %d is not alloced\n", fd);
			return -EBADF;
		}

		pr_debug("free fd %d for real_fd %d cid %"PRId64" from fds %d\n",
			 fd, fds->fd_maps[offs].real_fd, fd,
			 fds->fd_maps[offs].cid, fds->start_fd);
		if (map != NULL) {
			map->real_fd = fds->fd_maps[offs].real_fd;
			map->cid = fds->fd_maps[offs].cid;
		}
		fds->fd_maps[offs].real_fd = -1;
		fds->fd_maps[offs].cid = -1;
		fds->free_nr++;

		ci->total_free_fd_nr++;

		break;
	}

	pthread_rwlock_unlock(&ci->rwlock);

	return 0;
}
