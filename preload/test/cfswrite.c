#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

#include "libcfs.h"

int main(int argc, char *argv[])
{
	int64_t cid;

	char *filename = argv[1];
	int blksize = atoi(argv[2]);
	int count = atoi(argv[3]);
	char buf[1024];
	ssize_t wsize;
	int err, fd;

	cid = cfs_new_client();
	if (cid < 0) {
		printf("failed to create cid: %lld\n", (long long)cid);
		return -1;
	}
	err = cfs_set_client(cid, "volName", "ltptest");
	if (err < 0) {
		printf("failed to set volName: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "masterAddr", "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010");
	if (err < 0) {
		printf("failed to set masteraddr: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "followerRead", "true");
	if (err < 0) {
		printf("failed to set followerread: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "logDir", "/tmp/cfs");
	if (err < 0) {
		printf("failed to set logdir: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "logLevel", "info");
	if (err < 0) {
		printf("failed to set loglevel: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "user", "ltptest");
	if (err < 0) {
		printf("failed to set user: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "ak", "39bEF4RrAQgMj6RV");
	if (err < 0) {
		printf("failed to set ak: %d\n", err);
		return err;
	}
	err = cfs_set_client(cid, "sk", "TRL6o3JL16YOqvZGIohBDFTHZDEcFsyd");
	if (err < 0) {
		printf("failed to set sk: %d\n", err);
		return err;
	}


	err = cfs_start_client(cid);
	if (err < 0) {
		printf("failed to start client: %d\n", err);
		goto out;
	}

	printf("cid %lld Write [%s] blksize %dK count %d\n", (long long)cid, filename, blksize, count);
	fd = cfs_open(cid, filename, O_CREAT | O_RDWR, 0666);
	if (fd < 0) {
		printf("failed to open: %d\n", fd);
		goto out;
	}

	off_t off = 0;
	for (int i = 0; i < count; i++) {
		wsize = cfs_write(cid, fd, buf, 1024, off);
		if (wsize != 1024) {
			printf("failed to write: %d\n", fd);
		}
		off += 1024;
	}

	cfs_close(cid, fd);
out:
	cfs_close_client(cid);
	return 0;
}
