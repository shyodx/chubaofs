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
#include <stdatomic.h>
#include <sched.h>
#include <errno.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>

#include "common.h"
#include "queue.h"
#include "apis.h"
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
#define QUEUE_CMD_REGISTER_CLONE	2
#define QUEUE_CMD_UNREGISTER		3

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

		/* for register clone */
		struct {
			__be64 app_id;		// which client that the new client will attach to
		} __attribute__((packed));

		/* for unregister */
		struct {
			__be64 unreg_id;	// which client to unregister
		} __attribute__((packed));
	};
	char data[0];	// save all types of queue_info_ipc for register cmds
} __attribute__((packed));

#define WRITE_ONCE(var, val)						\
	atomic_store_explicit((_Atomic __typeof__(var) *)&(var),	\
			      (val), memory_order_relaxed)
#define READ_ONCE(var)						\
	atomic_load_explicit((_Atomic __typeof__(var) *)&(var),	\
			     memory_order_relaxed)

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
	ret = orig_apis.ftruncate(fd, shm_size);
	if (ret < 0) {
		pr_error("Failed to truncate shm file %s: %d\n", queue->name, errno);
		orig_apis.close(fd);
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
		orig_apis.close(fd);
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
	orig_apis.close(fd);

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

void queue_destroy(struct queue_info *queue)
{
	if (queue == NULL)
		return;

	pr_debug("destroy queue %d addr %p name %s size %zu\n",
		 queue->type, queue->addr, queue->name, queue->shm_size);

	if (queue->addr) {
		if (queue->type == CTRL_QUEUE) {
			uint32_t head, tail, flags;

			head = READ_ONCE(*queue->hdr.head);
			tail = READ_ONCE(*queue->hdr.tail);
			flags = READ_ONCE(*queue->hdr.flags);

			if (head != tail)
				pr_warn("queue(type:%d head:%u tail:%u flags:%x) is not empty\n",
					queue->type, head, tail, flags);
		}

		munmap(queue->addr, queue->shm_size);
	}
	if (queue->name) {
		shm_unlink(queue->name);
		free(queue->name);
	}
	free(queue->bitmap);
	free(queue);
}

int queue_unregister(int sockfd, uint64_t id)
{
	struct queue_cmd_ipc cmd;
	char data[8] = {0};
	int ret;

	memset(&cmd, 0, sizeof(cmd));
	cmd.cmd = htobe16((uint16_t)QUEUE_CMD_UNREGISTER);
	cmd.unreg_id = htobe64((uint64_t)id);

	ret = orig_apis.write(sockfd, &cmd, sizeof(cmd));
	if (ret < 0) {
		pr_error("Failed to write data to socket: %d\n", errno);
		return ret;
	}
	pr_debug("write data %d bytes to server\n", ret);

	ret = orig_apis.read(sockfd, data, sizeof(data));
	if (ret < 0) {
		pr_error("Failed to read data from socket: %d\n", errno);
		return ret;
	}
	pr_debug("read data %d bytes from server\n", ret);

	if (ret != sizeof(uint64_t)) {
		pr_warn("invalid return value size %d\n", ret);
		for (int i = 1; i <= ret; i++) {
			if (i % 32 == 0)
				printf("\n");
			printf("%02d ", ((unsigned char *)data)[i - 1]);
		}
		printf("\n");
	}

	swap_be64_to_cpu((unsigned char *)data);
	int64_t retval = *((int64_t *)data);
	if (retval < 0) {
		pr_error("Invalid retval %"PRIx64"\n", retval);
		return -ERANGE;
	}

	return 0;
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

	ret = orig_apis.write(sockfd, out_data, size);
	if (ret < 0) {
		pr_error("Failed to write data to socket: %d\n", errno);
		free(out_data);
		return ret;
	}
	pr_debug("write data %d bytes to server\n", ret);

	assert(size > sizeof(uint64_t));
	ret = orig_apis.read(sockfd, out_data, size);
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

int queue_register_clone(int sockfd, struct queue_info *queue_array[QUEUE_TYPE_NR], uint64_t *id)
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

        out_data = malloc(size);
        if (out_data == NULL)
                return -ENOMEM;
	memset(out_data, 0, size);

	cmd = (struct queue_cmd_ipc *)out_data;
	cmd->cmd = htobe16((uint16_t)QUEUE_CMD_REGISTER_CLONE);
	cmd->app_id = htobe64(*id);
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

	ret = orig_apis.write(sockfd, out_data, size);
	if (ret < 0) {
		pr_error("Failed to write data to socket: %d\n", errno);
		free(out_data);
		return ret;
	}
	pr_debug("write data %d bytes to server\n", ret);

	assert(size > sizeof(uint64_t));
	ret = orig_apis.read(sockfd, out_data, size);
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

static int find_items(struct queue_info *queue, int nr, uint32_t *start)
{
	int alloced = 0;
	uint32_t prev_idx = 0, idx;
	int loop = 0;

	pr_debug("queue type %d nr %d next %u used %u members %u\n", queue->type, nr, queue->next, queue->used, queue->members);

	assert(queue->type == DATA_QUEUE || (queue->type == DONE_QUEUE && nr == 1));

	pthread_mutex_lock(&queue->mutex);
	while (queue->used == queue->members) {
		pr_debug("queue[%d] is full used %"PRIu32" try again %d",
			 queue->type, queue->used, loop++);
		pthread_cond_wait(&queue->cond, &queue->mutex);
	}

	while (queue->used < queue->members && alloced < nr) {
		idx = find_next_zero_bit(queue->bitmap, queue->next, queue->members);
		if (idx == queue->members) {
			/* hit the end of bitmap */
			idx = find_first_zero_bit(queue->bitmap, queue->members);
		}
		if (idx == queue->members)
			break;
		if (queue->type == DATA_QUEUE) {
			struct data_item *array = (struct data_item *)queue->data;
			array[idx].next = alloced == 0 ? DATA_SLOT_END : prev_idx;
		}
		set_bit(queue->bitmap, idx);
		alloced++;
		queue->used++;
		queue->next = (idx + 1) & queue->mask; // FIXME: +1 or not, need check where find_next_zero starts
		prev_idx = idx;
	}
	*start = prev_idx;
	pr_debug("queue type %d get free slot %u alloced %d expected %d\n",
		 queue->type, *start, alloced, nr);
	pthread_mutex_unlock(&queue->mutex);
	return alloced;
}

static void free_items(struct queue_info *queue, uint32_t start)
{
	uint32_t next;

	assert(queue->type != CTRL_QUEUE);

	pthread_mutex_lock(&queue->mutex);
	if (queue->type == DONE_QUEUE) {
		assert(test_bit(queue->bitmap, start));
		clear_bit(queue->bitmap, start);
		queue->used--;
	} else {
		struct data_item *array = (struct data_item *)queue->data;
		uint64_t req_id = array[start].req_id;
		do {
			assert(test_bit(queue->bitmap, start));
			assert(req_id == array[start].req_id);
			req_id = array[start].req_id;
			next = array[start].next;
			clear_bit(queue->bitmap, start);
			queue->used--;
			start = next;
		} while (start != DATA_SLOT_END);
	}
	pthread_mutex_unlock(&queue->mutex);
}

static int find_next_item(struct queue_info *queue, uint32_t *idx)
{
	uint32_t head, tail;
	int loop = 0;

	assert(queue->type == CTRL_QUEUE);

	pthread_mutex_lock(&queue->mutex);

	head = READ_ONCE(*queue->hdr.head);
	tail = READ_ONCE(*queue->hdr.tail);
	while (IS_FULL(head, tail, queue->members)) {
		pr_debug("queue[%d] is full head %"PRIu32" tail %"PRIu32", try again %d\n",
			 queue->type, head, tail, loop++);
		pthread_cond_wait(&queue->cond, &queue->mutex);
		head = READ_ONCE(*queue->hdr.head);
		tail = READ_ONCE(*queue->hdr.tail);
	}

	*idx = tail & queue->mask;
	tail++;

	WRITE_ONCE(*queue->hdr.tail, tail);

	pr_debug("queue[%d] get item %u\n", queue->type, *idx);

	pthread_mutex_unlock(&queue->mutex);
	return 0;
}

static void put_item(struct queue_info *queue)
{
	uint32_t head, tail;
	assert(queue->type == CTRL_QUEUE);

	pthread_mutex_lock(&queue->mutex);

	head = READ_ONCE(*queue->hdr.head);
	tail = READ_ONCE(*queue->hdr.tail);
	if (IS_EMPTY(head, tail)) {
		pr_error("queue[%d] is empty? head/tail %"PRIu32"\n",
			 queue->type, head);
		assert(0);
	}

	head++;
	WRITE_ONCE(*queue->hdr.head, head);

	pthread_mutex_unlock(&queue->mutex);
	pthread_cond_signal(&queue->cond);
}

/* FIXME: queue must be referenced to avoid being unmapped? */
int queue_get_items(struct queue_info *queue_array[QUEUE_TYPE_NR], struct queue_item_params *params)
{
	struct queue_info *ctrl, *data, *done; 
	uint32_t ctrl_idx, data_idx, done_idx;
	int ret;

	if (queue_array == NULL || params == NULL) {
		pr_error("Invalid params: queue_array %s params %s\n",
			 queue_array == NULL ? "(null)" : "valid",
			 params == NULL ? "(null)" : "valid");
		return -EINVAL;
	}

	ctrl = queue_array[CTRL_QUEUE];
	data = queue_array[DATA_QUEUE];
	done = queue_array[DONE_QUEUE];
	if (ctrl == NULL || data == NULL || done == NULL) {
		pr_error("Invalid params: ctrl %s data %s done %s\n",
			 ctrl == NULL ? "(null)" : "valid",
			 data == NULL ? "(null)" : "valid",
			 done == NULL ? "(null)" : "valid");
		return -EINVAL;
	}

	if (params->data_expected) {
		ret = find_items(data, params->data_expected, &data_idx);
		if (ret < 0) {
			pr_error("Not enough slots in data queue\n");
			return ret;
		}

		params->data_alloced = ret;
		params->data_idx = data_idx;
	}

	ret = find_items(done, 1, &done_idx);
	if (ret <= 0) {
		pr_error("Not enough slots in done queue\n");
		free_items(data, data_idx);
		return ret;
	}
	params->done_idx = done_idx;

	ret = find_next_item(ctrl, &ctrl_idx);
	if (ret < 0) {
		pr_error("Ctrl queue is full\n");
		free_items(done, done_idx);
		free_items(data, data_idx);
		return ret;
	}
	params->ctrl_idx = ctrl_idx;

	return 0;
}

void queue_put_items(struct queue_info *queue_array[QUEUE_TYPE_NR], struct ctrl_item *item)
{
	if (item == NULL)
		return;

	if (item->state & CTRL_STATE_DATA_ITEM) {
		pr_debug("Req[%"PRIu64"] free data items from %u\n", item->req_id, item->data_idx);
		free_items(queue_array[DATA_QUEUE], item->data_idx);
	}

	pr_debug("Req[%"PRIu64"] free done item %u\n", item->req_id, item->done_idx);
	free_items(queue_array[DONE_QUEUE], item->done_idx);

	pr_debug("Req[%"PRIu64"] put ctrl item\n", item->req_id);
	put_item(queue_array[CTRL_QUEUE]);
}

int queue_poll_item(struct queue_info *queue, struct done_item *item)
{
	int ret = 0;
	assert(queue->type == CTRL_QUEUE);

	while (1) {
		/* FIXME: atomic load? */
		uint32_t done_state = READ_ONCE(item->state);
		if (done_state == DONE_STATE_READY) {
			print_done_item(item);
			return item->retval;
		}

		sched_yield();
		//pr_debug("Req[%"PRIu64"] poll done item state %u\n", item->req_id, item->state);
		//sleep(1);
	}
	return ret;
}

void queue_mark_item_ready(struct ctrl_item *item)
{
	uint32_t state;

	state = item->state | CTRL_STATE_READY;
	/* FIXME: atomic set? need barrier? */
	WRITE_ONCE(item->state, state);
}

void *queue_item(struct queue_info *queue, uint32_t idx)
{
	void *item = NULL;

	if (idx >= queue->members)
		return NULL;

	switch (queue->type) {
	case CTRL_QUEUE:
		item = (void *)(&((struct ctrl_item *)queue->data)[idx]);
		break;
	case DATA_QUEUE:
		item = (void *)(&((struct data_item *)queue->data)[idx]);
		break;
	case DONE_QUEUE:
		item = (void *)(&((struct done_item *)queue->data)[idx]);
		break;
	}

	return item;
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
	return orig_apis.close(sockfd);
}
