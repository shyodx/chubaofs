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
//
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdarg.h>
#include <errno.h>

#include "common.h"
#include "client.h"
#include "fd_map.h"
#include "apis.h"
#include "log.h"

struct rw_params {
	uint32_t fd;
	uint32_t rsvd;
	uint64_t count;
	uint64_t offs;
	char data[0];
} __attribute__((packed));

static ssize_t cfs_read(int64_t cid, struct fd_map *map, void *buf, size_t count)
{
	struct queue_info *ctrl = map->queue_array[CTRL_QUEUE];
	struct queue_info *data = map->queue_array[DATA_QUEUE];
	struct queue_info *done = map->queue_array[DONE_QUEUE];
	struct ctrl_item *ctrl_item = NULL;
	struct data_item *data_item = NULL;
	struct done_item *done_item = NULL;
	uint64_t req_id;
	struct rw_params *read_params;
	size_t params_size = sizeof(struct rw_params) + count;
	struct queue_item_params item_params = {0};
	ssize_t rsize = 0;
	size_t left, copy_size;
	bool no_more = 0;
	int ret;

	if (params_size > CTRL_DATA_SIZE) {
		/* need data items to save read data */
		item_params.data_expected = count / DATA_DATA_SIZE;
		if (count % DATA_DATA_SIZE > 0)
			item_params.data_expected++;
		pr_debug("Expected %d data items\n", item_params.data_expected);
	}

	left = count;
	while (left > 0) {
		req_id = next_req_id();
		pr_debug("Get Req[%"PRIu64"]\n", req_id);

		ret = queue_get_items(map->queue_array, &item_params);
		if (ret < 0)
			return ret;

		/* FIXME: how to restore alloced ctrl item if fail, add state to mark it as invalidate? */
		ctrl_item = (struct ctrl_item *)queue_item(ctrl, item_params.ctrl_idx);
		assert(ctrl_item->state == CTRL_STATE_NEW);
		ctrl_item->req_id = req_id;
		ctrl_item->opcode = CBFS_READ;
		ctrl_item->data_idx = item_params.data_idx;
		ctrl_item->done_idx = item_params.done_idx;
		pr_debug("Req[%"PRIu64"] Get ctrl idx %u\n", req_id, item_params.ctrl_idx);

		done_item = (struct done_item *)queue_item(done, item_params.done_idx);
		init_done_item(done_item, req_id, CBFS_READ);
		pr_debug("Req[%"PRIu64"] Get done idx %u\n", req_id, item_params.done_idx);

		read_params = (struct rw_params *)ctrl_item->data;
		read_params->fd = (uint32_t)map->real_fd;
		read_params->count = (uint64_t)left;
		read_params->offs = map->offset + rsize;
		if (item_params.data_expected > 0) {
			for_each_data_item(data, data_item, item_params.data_idx) {
				init_data_item(data_item, req_id, CBFS_READ/*, params_size*/);
			}
			ctrl_item->state |= CTRL_STATE_DATA_ITEM;
			pr_debug("Req[%"PRIu64"] Get data idx %u\n", req_id, item_params.data_idx);
		} else {
			ctrl_item->state |= CTRL_STATE_INLINE_DATA;
			pr_debug("Req[%"PRIu64"] Set ctrl idx %u inline data\n", req_id, item_params.ctrl_idx);
		}

		/* update ctrl state at the end of preparation */
		queue_mark_item_ready(ctrl_item);
		pr_debug("Req[%"PRIu64"] Mark ctrl idx %u ready\n", req_id, item_params.ctrl_idx);
		//ctrl_item_tostring(ctrl_item, buf);

		/* wait for done */
		ret = queue_poll_item(ctrl, done_item);
		pr_debug("Req[%"PRIu64"] ctrl idx %u done idx %u get return %d\n",
			 req_id, item_params.ctrl_idx, item_params.done_idx, ret);
		if (ret < 0) {
			pr_error("Req[%"PRIu64"] read at offs %zd fail: %d\n", req_id, rsize, ret);
			rsize = (ssize_t)ret;
			break;
		} else if ((uint64_t)ret < left) {
			pr_debug("Req[%"PRIu64"] read at offs %zd return %d, no more to read\n",
				 req_id, rsize, ret);
			no_more = true;
		}

		/* copy data to buffer */
		copy_size = 0;
		if (item_params.data_expected) {
			for_each_data_item(data, data_item, item_params.data_idx) {
				if (ret == 0) {
					if (data_item->next != DATA_SLOT_END)
						pr_warn("Still has data_item not used\n");
					break;
				}
				copy_size = min(left, DATA_DATA_SIZE);
				copy_size = min(copy_size, ret);
				memcpy(buf + rsize, data_item->data, copy_size);
				pr_debug("copy to buffer offs %zd size %zu from data item\n", rsize, copy_size);
				for (int i = 0; i < 32; i++) {
					printf(" %02x", data_item->data[i]);
				}
				printf("\n");
				rsize += copy_size;
				left -= copy_size;
				ret -= copy_size;
			}
		} else {
			copy_size = min(ret, DATA_DATA_SIZE - sizeof(struct rw_params));
			memcpy(buf + rsize, read_params->data, copy_size);
			pr_debug("copy to buffer offs %zd size %zu from inline data\n", rsize, copy_size);
			rsize += copy_size;
			left -= copy_size;
		}

		queue_put_items(map->queue_array, ctrl_item);

		if (left == 0 || no_more)
			break;

		pr_debug("Left %zu need read more\n", left);
		if (sizeof(struct rw_params) + left > CTRL_DATA_SIZE) {
			memset(&item_params, 0, sizeof(struct queue_item_params));
			// FIXME: if DATA_DATA_SIZE is power of 2, we can use shift here
			item_params.data_expected = left / DATA_DATA_SIZE;
			if (left % DATA_DATA_SIZE > 0)
				item_params.data_expected++;
		}
	}

	return rsize;
}

ssize_t read(int fd, void *buf, size_t count)
{
	struct client_info *ci = get_client(getpid());
	struct fd_map map = {0};
	ssize_t ret;

	pr_debug("client %"PRId64" read fd %d\n", ci->appid, fd);

	ret = (ssize_t)query_fd(ci, fd, &map);
	if (ret < 0) {
		pr_error("Failed to get real fd of fd %d: %s\n", fd, strerror((int)(-ret)));
		goto out;
	}

	if (map.cid >= 0) {
		pr_debug("fd %d real_fd %d offset %jd count %zu is in cfs path cid %"PRId64"\n",
			 fd, map.real_fd, map.offset, count, map.cid);
		ret = cfs_read(map.cid, &map, buf, count);
	} else {
		pr_debug("fd %d real_fd %d is NOT in cfs path\n",
			 fd, map.real_fd);
		ret = orig_apis.read(map.real_fd, buf, count);
	}
	pr_debug("read fd %d size %zd buf %s\n", fd, ret, (char *)buf);

	if (ret > 0) {
		map.offset += ret;
		update_fd(ci, fd, &map);
	}

out:
	put_client(ci);
	SET_ERRNO(ret);
	return ret < 0 ? -1 : ret;
}
