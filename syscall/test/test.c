#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define BUF_SIZE 64

int main(int argc, char **argv)
{
	int fd;
	char buf[BUF_SIZE] = {0};
	ssize_t size;

	fd = open(argv[1], O_RDWR);
	if (fd < 0) {
		perror("open");
		return fd;
	}

	size = read(fd, buf, BUF_SIZE);
	if (size < 0) {
		perror("read");
	}

	printf("read %zd bytes: '%s'\n", size, buf);

	memcpy(buf, "\nAgain", 6 + 1);
	size = write(fd, buf, 6);
	if (size < 0) {
		perror("write");
	}

	printf("write %zd bytes\n", size);

	close(fd);
	return 0;
}
