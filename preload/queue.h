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

#ifndef _CFS_PRELOAD_LIB_QUEUE_H
#define _CFS_PRELOAD_LIB_QUEUE_H

#include <string.h>
#include <stdint.h>
#include <assert.h>

#include "log.h"

enum queue_type {
	CTRL_QUEUE,
	DATA_QUEUE,
	DONE_QUEUE,
	QUEUE_TYPE_NR
};

#define QUEUE_MIN_ORDER	1
#define QUEUE_MAX_ORDER	4096

// FIXME: need get the right size and number
#define CTRL_QUEUE_MEMBERS_ORDER 10
#define DATA_QUEUE_MEMBERS_ORDER 10
#define DONE_QUEUE_MEMBERS_ORDER 10

#define QUEUE_SUSPEND 0x1

#define QUEUE_HDR_SIZE sizeof(struct queue_header)
struct queue_header {
	uint32_t head;		// where to release item
	uint32_t tail;		// where to insert item
	uint32_t flags;
	uint32_t reserved;
};

struct queue_header_ptr {
	uint32_t *head;		// point to queue_header.head
	uint32_t *tail;		// point to queue_header.tail
	uint32_t *flags;
};

struct mountpoint;
struct queue_info {
	uint16_t type;		// queue type
	uint16_t size;		// member size
	uint32_t members;	// number of members
	uint32_t mask;
	size_t shm_size;
	uint32_t used;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint32_t next;		// next bit to scan
	unsigned long *bitmap;	// 1: used 0: available, only used by data and done
	char *name;		// shm file name
	struct mountpoint *mnt;

	void *addr;		// shared memory address
	struct queue_header_ptr hdr;	// queue header, data has no header
	void *data;		// items start from here
};

struct queue_item_params {
	/* input */
	int data_expected;

	/* output */
	int ctrl_idx;
	int done_idx;
	int data_idx;
	int data_alloced;
};

#define IS_FULL(head, tail, nmemb) (((tail) > (head) && (tail) - (head) == nmemb) || \
				    ((tail) < (head) && (head) - (tail) == nmemb))
#define IS_EMPTY(head, tail) ((head) == (tail))

#define CTRL_STATE_NEW		0U
#define CTRL_STATE_READY	0x00000001U
#define CTRL_STATE_DATA_ITEM	0x10000000U
#define CTRL_STATE_INLINE_DATA	0x20000000U

/* FIXME: need a BUILD_BUG_ON to check struct size */
#define CTRL_ITEM_SIZE 64
#define CTRL_DATA_SIZE 40
/* each ctrl is 64 byte */
struct ctrl_item {
	uint64_t req_id;	// unique id for request
	uint32_t opcode;
	uint32_t state;		// atomic: DATA_ITEM | INLINE_DATA
	uint32_t done_idx;	// where to put return value
	uint32_t data_idx;	// start from this slot in data queue
	char data[CTRL_DATA_SIZE];	// for short params and data
};

///* FIXME: buf is 1024 */
//#include <string.h>
//static inline char *ctrl_item_tostring(struct ctrl_item *item, char *buf)
//{
//	char path[64] = {0};
//	int offs;
//struct open_params {
//	uint32_t flags;
//	uint32_t mode;
//	uint16_t pathlen;
//	char path[0];
//} __attribute__((packed));
//
//
//	offs = sprintf(buf, "ctrl_item(req_id:%lu opcode:%u state:%#x done_idx:%u data_idx:%u",
//		 (unsigned long)item->req_id, item->opcode, item->state, item->done_idx, item->data_idx);
//
//	if (item->state & CTRL_STATE_INLINE_DATA) {
//		struct open_params *p = (struct open_params *)item->data;
//		memcpy(path, p->path, p->pathlen);
//		sprintf(buf + offs, " flags:%x mode:%o path:%s)", p->flags, p->mode, path);
//	} else {
//		sprintf(buf + offs, " data:<in data item>)");
//	}
//
//	return buf;
//}

#define DONE_STATE_NEW		0U
#define DONE_STATE_READY	1U
#define DONE_STATE_STALE	2U

#define DONE_ITEM_SIZE 24
struct done_item {
	uint64_t req_id;	// the same with ctrl_item
	uint32_t opcode;
	uint32_t state;		// atomic: FRESH or STALE
	int64_t retval;
	//uint32_t ctrl_idx;	// slot in ctrl queue
	//uint32_t data_idx;	// start from this tlot in data queue
	//uint64_t size;		// data size
};

#define DATA_SLOT_END ~0

// FIXME: DATA_DATA_SIZE should be 4096, DATA_ITEM_SIZE could be larger than 4096
#define DATA_ITEM_SIZE 4096
#define DATA_DATA_SIZE 4080//4076
/* each data_item is 4096 byte */
struct data_item {
	uint64_t req_id;	// the same with ctrl_item
	uint32_t opcode;
	uint32_t next;		// next data item, 0xffffffff means end of chain
	//uint32_t rsvd;		// data size
	char data[DATA_DATA_SIZE];	// contains params and data according to opcode
};

static inline void init_data_item(struct data_item *item,
				  uint64_t req_id, uint32_t opcode/*,
				  uint32_t size*/)
{
	item->req_id = req_id;
	item->opcode = opcode;
	//item->size = size;
	//assert(size <= DATA_DATA_SIZE);
}

static inline void init_done_item(struct done_item *item,
				  uint64_t req_id, uint32_t opcode)
{
	item->req_id = req_id;
	item->opcode = opcode;
	item->retval = 0;
	// FIXME: need atomic and barrier?
	item->state = DONE_STATE_NEW;
}

static inline void print_done_item(struct done_item *item)
{
	pr_info("done_item: req_id(%"PRIu64"), opcode(%"PRIu32"), "
		"retval(%"PRId64"), state(%"PRIu32")\n", item->req_id,
		item->opcode, item->retval, item->state);
}

int queue_create(enum queue_type type, unsigned int nmemb_order, struct queue_info **queue);
int queue_register(int sockfd, struct queue_info *queue_array[QUEUE_TYPE_NR], uint64_t *id);
int queue_unregister(int sockfd, uint64_t id);
void queue_destroy(struct queue_info *queue);
int queue_get_items(struct queue_info *queue_array[QUEUE_TYPE_NR], struct queue_item_params *params);
void queue_put_items(struct queue_info *queue_array[QUEUE_TYPE_NR], struct ctrl_item *item);
int queue_poll_item(struct queue_info *ctrl_queue, struct done_item *item);
void queue_mark_item_ready(struct ctrl_item *item);
void *queue_item(struct queue_info *queue, uint32_t idx);

int connect_to_daemon(const char *fsname);
int disconnect_to_daemon(int sockfd);

#endif
