#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv)
{
	int fd;

	fd = open(argv[1], O_RDWR);
	if (fd < 0) {
		return fd;
	}

	close(fd);
	return 0;
}
