#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

int main(int argc, char *argv[])
{
	char *filename = argv[1];
	int blksize = atoi(argv[2]);
	int count = atoi(argv[3]);
	char buf[1024];
	ssize_t wsize;
	int fd;

	printf("Write [%s] blksize %dK count %d\n", filename, blksize, count);
	fd = open(filename, O_CREAT | O_RDWR, 0666);
	if (fd < 0) {
		perror("open");
		return fd;
	}

	for (int i = 0; i < count; i++) {
		wsize = write(fd, buf, 1024);
		if (wsize != 1024) {
			perror("write");
		}
	}

	close(fd);
	return 0;
}

