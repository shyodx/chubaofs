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

#include <pthread.h>
#include <fcntl.h>
#include <endian.h>
#include <limits.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <stddef.h>
#include <errno.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>

#include "common.h"
#include "queue.h"
#include "log.h"

struct queue_attr {
	const char *basename;
	uint16_t size;
	uint16_t hdr_size;
	int oflags;
	int prot;
};

const struct queue_attr queue_attr[] = {
	{ "ctrl", CTRL_ITEM_SIZE, QUEUE_HDR_SIZE, O_RDWR, PROT_WRITE | PROT_READ },
	{ "data", DATA_ITEM_SIZE, 0, O_RDWR, PROT_WRITE | PROT_READ },
	{ "done", DONE_ITEM_SIZE, 0, O_RDWR, PROT_WRITE | PROT_READ },
};

typedef uint16_t __be16;
typedef uint32_t __be32;
typedef uint64_t __be64;

#define QUEUE_CMD_REGISTER_NEW		1

struct queue_info_ipc {
	__be16 type;
	__be16 namelen;
	__be16 size;
	__be32 members;
	char name[0];
} __attribute__((packed));

struct queue_cmd_ipc {
	__be16 cmd;
	__be16 rsvd1;
	__be32 rsvd2;
	union {
		/* for register new */
		struct {
			__be64 rsvd3;
		} __attribute__((packed));
	};
	char data[0];	// save all types of queue_info_ipc for register cmds
} __attribute__((packed));

static inline void swap_be64_to_cpu(unsigned char *data)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
	int start;
	int end = (int)(sizeof(uint64_t) - 1);
	char tmp;
	for (start = 0; start < sizeof(uint64_t); start++)
		printf("%02d ", data[start]);
	printf("\n");
	for (start = 0; start < sizeof(uint64_t) / 2; start++, end--) {
		tmp = data[start];
		data[start] = data[end];
		data[end] = tmp;
	}
	for (start = 0; start < sizeof(uint64_t); start++)
		printf("%02d ", data[start]);
	printf("\n");
#endif
}

static int init_queue_shm(struct queue_info *queue)
{
	int oflag = O_CREAT | O_EXCL;
	int fd;
	size_t shm_size;
	int shm_prot;
	int ret;

	oflag |= queue_attr[queue->type].oflags;
	fd = shm_open(queue->name, oflag, 0666);
	if (fd < 0) {
		pr_error("Failed to create shm file %s: %d\n", queue->name, errno);
		return fd;
	}

	shm_size = queue_attr[queue->type].hdr_size + queue->size * queue->members;
	shm_size = round_up(shm_size, PAGE_SIZE);
	queue->shm_size = shm_size;
	/* FIXME: need a warning if shm_size too large? */

	/* FIXME: shall we truncate to 0 first to make sure file content is all zero? */
	ret = ftruncate(fd, shm_size);
	if (ret < 0) {
		pr_error("Failed to truncate shm file %s: %d\n", queue->name, errno);
		close(fd);
		shm_unlink(queue->name);
		return ret;
	}

	pr_debug("shm file %s map size %zu\n", queue->name, shm_size);
	shm_prot = queue_attr[queue->type].prot;
	// FIXME: need MAP_LOCKED to resident memory?
	queue->addr = mmap(NULL, shm_size, shm_prot, MAP_SHARED, fd, 0);
	if (queue->addr == MAP_FAILED) {
		queue->addr = NULL;
		queue->shm_size = 0;
		ret = -errno;
		pr_error("Failed to mmap file %s size %zu: %d\n",
			 queue->name, shm_size, errno);
		close(fd);
		shm_unlink(queue->name);
		return ret;
	}

	if (queue_attr[queue->type].hdr_size > 0) {
		struct queue_header *hdr = (struct queue_header *)queue->addr;

		pr_debug("Init queue header for queue %d\n", queue->type);
		queue->hdr.head = &hdr->head;
		queue->hdr.tail = &hdr->tail;
		queue->hdr.flags = &hdr->flags;
		queue->data = queue->addr + sizeof(struct queue_header);
	} else {
		queue->data = queue->addr;
	}

	/* unlink shm file after register queue successfully */
	close(fd);

	return 0;
}

static inline char *gen_shm_name(const char *base)
{
	char path[PATH_MAX] = {0};
	int ret;

	srand(time(NULL));
	ret = snprintf(path, PATH_MAX, "%s.%d.%d", base, getpid(), rand());
	assert(ret > 0 && ret < PATH_MAX);
	pr_debug("new shm file %s\n", path);
	return strdup(path);
}

int queue_create(enum queue_type type, unsigned int nmemb_order, struct queue_info **queue)
{
	struct queue_info *q;
	int ret = 0;

	if (queue == NULL)
		return -EINVAL;
	if (type >= QUEUE_TYPE_NR)
		return -EINVAL;
	if (nmemb_order < QUEUE_MIN_ORDER || nmemb_order > QUEUE_MAX_ORDER)
		return -EINVAL;

	/* FIXME: should use BUILD_BUG_ON */
        assert(sizeof(struct ctrl_item) == CTRL_ITEM_SIZE);
        assert(sizeof(struct done_item) == DONE_ITEM_SIZE);
        assert(sizeof(struct data_item) == DATA_ITEM_SIZE);

	q = malloc(sizeof(struct queue_info));
	if (q == NULL) {
		pr_error("alloc queue_info fail: %d\n", errno);
		return -ENOMEM;
	}
	memset(q, 0, sizeof(struct queue_info));

	q->type = type;
	q->size = queue_attr[type].size;
	q->members = 1 << nmemb_order;
	q->mask = q->members - 1;
	q->next = 0;
	q->name = gen_shm_name(queue_attr[type].basename);
	if (q->name == NULL) {
		free(q);
		return -ENOMEM;
	}
	if (type > CTRL_QUEUE) {
		q->bitmap = alloc_bitmap(q->members);
		if (q->bitmap == NULL) {
			pr_error("alloc queue %d bitmap fail: %d\n", type, errno);
			free(q->name);
			free(q);
			return -ENOMEM;
		}
	}
	pthread_mutex_init(&q->mutex, NULL);
	pthread_cond_init(&q->cond, NULL);

	ret = init_queue_shm(q);
	if (ret < 0) {
		free(q->bitmap);
		free(q->name);
		free(q);
	}

	*queue = q;

	return ret;
}

int queue_register(int sockfd, struct queue_info *queue_array[QUEUE_TYPE_NR], uint64_t *id)
{
	struct queue_cmd_ipc *cmd;
	struct queue_info_ipc *qdata;
	struct queue_info *queue;
	size_t namelen;
	void *out_data;
	size_t size, offs;
	int ret;

	if (queue_array == NULL || id == NULL)
		return -EINVAL;

	size = sizeof(struct queue_cmd_ipc);
	size += sizeof(struct queue_info_ipc) * QUEUE_TYPE_NR;
        for (int type = 0; type < QUEUE_TYPE_NR; type++)
                size += strlen(queue_array[type]->name);
	//size = round_up(size, 8);

        out_data = malloc(size);
        if (out_data == NULL)
                return -ENOMEM;
	memset(out_data, 0, size);

	cmd = (struct queue_cmd_ipc *)out_data;
	cmd->cmd = htobe16((uint16_t)QUEUE_CMD_REGISTER_NEW);
	offs = offsetof(struct queue_cmd_ipc, data);
	for (int type = 0; type < QUEUE_TYPE_NR; type++) {
		qdata = (struct queue_info_ipc *)(out_data + offs);
		queue = queue_array[type];
		namelen = strlen(queue->name);

		qdata->type = htobe16((uint16_t)queue->type);
		qdata->namelen = htobe16((uint16_t)namelen);
		qdata->size = htobe16(queue->size);
		qdata->members = htobe32(queue->members);
		strncpy(qdata->name, queue->name, namelen);

		pr_debug("queue(type:%d namelen:%zu size:%d members:%u name:%s) size %zu at offs %zu\n",
			 queue->type, namelen, queue->size, queue->members,
			 queue->name, sizeof(struct queue_info_ipc) + namelen, offs);

		offs += sizeof(struct queue_info_ipc) + namelen;
		assert(offs <= size);
	}

	ret = write(sockfd, out_data, size);
	if (ret < 0) {
		pr_error("Failed to write data to socket: %d\n", errno);
		free(out_data);
		return ret;
	}
	pr_debug("write data %d bytes to server\n", ret);

	assert(size > sizeof(uint64_t));
	ret = read(sockfd, out_data, size);
	if (ret < 0) {
		pr_error("Failed to read data from socket: %d\n", errno);
		free(out_data);
		return ret;
	}
	pr_debug("read data %d bytes from server\n", ret);

	if (ret != sizeof(uint64_t)) {
		pr_warn("invalid return value size %d\n", ret);
		for (int i = 1; i <= ret; i++) {
			if (i % 32 == 0)
				printf("\n");
			printf("%02d ", ((unsigned char *)out_data)[i - 1]);
		}
		printf("\n");
	}

	swap_be64_to_cpu((unsigned char *)out_data);
	*id = *((uint64_t *)out_data);
	free(out_data);
	if (*id == ULLONG_MAX) {
		pr_error("Invalid appid %"PRIx64"\n", *id);
		return -ERANGE;
	}

	pr_debug("get appid %"PRIu64"\n", *id);
	return 0;
}

int connect_to_daemon(const char *fsname)
{
	struct sockaddr_un addr;
	int sockfd;
	int ret;

	sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sockfd < 0) {
		pr_error("Failed to create socket: %d\n", errno);
		return sockfd;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	memcpy(addr.sun_path, SOCKADDR, strlen(SOCKADDR));

	ret = connect(sockfd, (const struct sockaddr *)&addr, sizeof(addr));
	if (ret < 0) {
		pr_error("Failded to connect %s: %d\n", SOCKADDR, errno);
		disconnect_to_daemon(sockfd);
		return ret;
	}

	return sockfd;
}

int disconnect_to_daemon(int sockfd)
{
	return close(sockfd);
}
