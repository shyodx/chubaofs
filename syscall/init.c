#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mntent.h>
#include <errno.h>

#include "client.h"
#include "fd_map.h"
#include "list.h"
#include "log.h"

#define __init __attribute__((constructor))
#define __exit __attribute__((destructor))

#define FSTYPE "fuse.chubaofs"

static int get_cfs_mount_info(struct client_info *ci)
{
	const char *mounts = "/proc/mounts";
	struct mntent *mnt;
	FILE *f;
	bool found_cfs = false;
	int fd;
	int ret = 0;

	f = setmntent(mounts, "r");
	if (f == NULL) {
		ret = -errno;
		pr_error("Failed to open %s: %s\n", mounts, strerror(errno));
		return ret;
	}

	while (1) {
		mnt = getmntent(f);
		if (mnt == NULL)
			break;

		pr_debug("fsname[%s] mnt_dir[%s] type[%s] opts[%s]\n",
			 mnt->mnt_fsname, mnt->mnt_dir, mnt->mnt_type, mnt->mnt_opts);
		if (strcmp(mnt->mnt_type, ci->fstype))
			continue;

		/* found cfs mount point */
		ret = append_mountpoint(ci, mnt->mnt_dir);
		if (ret != 0) {
			goto out;
		}

		found_cfs = true;
	}

	if (!found_cfs) {
		pr_error("CFS not found\n");
	}

out:
	endmntent(f);
	return ret;
}

static __init void init(void)
{
	struct client_info *ci;
	struct open_fd *open_fd, *next;
	LIST_HEAD(open_fd_list);
	int ret;

	pr_debug("Start init\n");

	ci = alloc_client(FSTYPE);
	if (ci == NULL) {
		pr_error("Failed to create cfs client\n");
		goto out;
	}

	/* if no cfs mounted, keep cfs_mountpoints as null
	 * FIXME: but how to deal with dynamic mount after app is initialized?
	 */
	ret = get_cfs_mount_info(ci);
	if (ret < 0) {
		pr_error("Failed to get cfs mount points: %s\n", strerror(ret));
		goto close_out;
	}

	ret = append_fd_map_set(ci);
	if (ret < 0) {
		pr_error("Failed to create fd map: %s\n", strerror(ret));
		goto close_out;
	}

	ret = get_opened_fd(ci, &open_fd_list);
	if (ret < 0) {
		pr_error("Failed to get opened fds: %s\n", strerror(ret));
		goto close_out;
	}

	list_for_each_entry_safe(open_fd, next, &open_fd_list, link) {
		/* FIXME: map all opened fd as not in_cfs? */
		ret = map_fd(ci, open_fd->fd, open_fd->fd, false);
		if (ret < 0) {
			pr_error("Failed to map fd: %s\n", strerror(ret));
			/* FIXME: need cleanup open_fd list */
			goto cleanup_fd_map_out;
		}

		free(open_fd);
	}

	ret = register_client(ci);
	if (ret < 0) {
		pr_error("Failed to register client: %s\n", strerror(ret));
		goto cleanup_fd_map_out;
	}

	return;

cleanup_fd_map_out:
	/* FIXME: cleanup will always be called when exit */
	//destroy_fd_map();
close_out:
	//close_cfs_client();
out:
	exit(ret);
}

static __exit void cleanup(void)
{
	pr_debug("Start cleanup all clients\n");
	destroy_all_clients();
}
