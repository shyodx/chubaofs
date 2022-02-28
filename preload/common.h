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

#ifndef _CFS_PRELOAD_LIB_COMMON_H
#define _CFS_PRELOAD_LIB_COMMON_H

#include <stdlib.h>
#include <limits.h>

#include "log.h"

#define FSTYPE "fuse.chubaofs"
#define SOCKADDR "/tmp/cfs/daemon/daemon.sock"

#define PAGE_SIZE 4096

#define dot "."
#define dotdot ".."

#define BITS_PER_BYTE_SHIFT	3
#define BYTES_PER_ULONG		sizeof(unsigned long)
#define BITS_PER_ULONG		(BYTES_PER_ULONG << BITS_PER_BYTE_SHIFT)

#define __round_mask(x, y) ((__typeof__(x))((y)-1))
#define round_up(x, y) ((((x)-1) | __round_mask(x, y))+1)

#define is_power_of_2(n) ((n) != 0 && ((n) & ((n) - 1)) == 0)

#define min(x, y) ((x) > (y) ? (y) : (x))

#define SET_ERRNO(ret) do { errno = (ret) < 0 ? -ret : 0; } while (0)

static inline unsigned long *alloc_bitmap(size_t size)
{
	unsigned long *ptr;
	size_t nr;

	nr = size / BITS_PER_ULONG;
	if (size & (BITS_PER_ULONG - 1))
		nr++;

	pr_debug("bitmap member %zu size %zu\n", size, nr);
	ptr = calloc(nr, BYTES_PER_ULONG);
	return ptr;
}

static inline unsigned long test_bit(unsigned long *bitmap, int n)
{
	int idx = n / BITS_PER_ULONG;
	int offs = n & (BITS_PER_ULONG - 1);

	return bitmap[idx] & (1 << offs);
}

static inline void set_bit(unsigned long *bitmap, int n)
{
	int idx = n / BITS_PER_ULONG;
	int offs = n & (BITS_PER_ULONG - 1);

	bitmap[idx] |= (1 << offs);
}

static inline void clear_bit(unsigned long *bitmap, int n)
{
	int idx = n / BITS_PER_ULONG;
	int offs = n & (BITS_PER_ULONG - 1);

	bitmap[idx] &= ~(1 << offs);
}

static inline int find_next_zero_bit(unsigned long *bitmap, int start, size_t size)
{
	int idx = start / BITS_PER_ULONG;
	int offs = start & (BITS_PER_ULONG - 1);
	int nr;
	int ret;

	nr = size / BITS_PER_ULONG;
	if (size & (BITS_PER_ULONG - 1))
		nr++;
	for (; idx < nr; idx++) {
		if (bitmap[idx] == ULONG_MAX)
			continue;
		for (; offs < BITS_PER_ULONG; offs++) {
			if ((bitmap[idx] & (1 << offs)) == 0) {
				ret = idx * BITS_PER_ULONG + offs;
				if (ret >= size)
					return size;
				return ret;
			}
		}
		offs = 0;
	}

	return size;
}

static inline int find_first_zero_bit(unsigned long *bitmap, size_t size)
{
	return find_next_zero_bit(bitmap, 0, size);
}

#endif
