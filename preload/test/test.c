#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#define BUF_SIZE 64

//#define TEST_RW

#define TEST_FORK

int main(int argc, char **argv)
{
	int fd;
	char buf[BUF_SIZE] = {0};
	ssize_t size;
	pid_t pid;

	fd = open(argv[1], O_RDWR);
	if (fd < 0) {
		perror("open");
		return fd;
	}

#ifdef TEST_RW
	size = read(fd, buf, BUF_SIZE);
	if (size < 0) {
		perror("TEST_RW: read");
	}

	printf("TEST_RW: read %zd bytes: '%s'\n", size, buf);

	memcpy(buf, "\nAgain", 6 + 1);
	size = write(fd, buf, 6);
	if (size < 0) {
		perror("TEST_RW: write");
	}
	printf("TEST_RW: write %zd bytes\n", size);

	size = read(fd, buf, BUF_SIZE);
	if (size < 0) {
		perror("TEST_RW: read");
	}
	buf[size] = 0;
	printf("TEST_RW: read %zd bytes after write: '%s'\n", size, buf);
#endif

#ifdef TEST_FORK
	pid = fork();
	if (pid < 0) {
		perror("parent: fork");
		close(fd);
		return pid;
	} else if (pid > 0) {
		int wstatus = 0;
		wait(&wstatus);
		printf("parent: wstatus: %d\n", wstatus);
		size = read(fd, buf, BUF_SIZE);
		if (size < 0) {
			perror("parent: read");
		}
		printf("parent: read %zd bytes: '%s'\n", size, buf);
		close(fd);
		return 0;
	}

	size = read(fd, buf, BUF_SIZE);
	if (size < 0) {
		perror("child: read");
	}

	printf("child: read %zd bytes: '%s'\n", size, buf);
	//printf("read %zd bytes: '%s' sleep 10s\n", size, buf);
	//sleep(10);

	//memcpy(buf, "\nAgain", 6 + 1);
	//size = write(fd, buf, 6);
	//if (size < 0) {
	//	perror("write");
	//}

	//printf("write %zd bytes\n", size);
#endif

	close(fd);
	return 0;
}
