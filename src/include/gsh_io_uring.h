/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file gsh_io_uring.h
 * @brief Shared thread-local io_uring ring for FSAL read/write.
 *
 * The ring, eventfd reaper, and submit accounting live here. Each FSAL
 * keeps its own file-descriptor handling, stable-write policy, and
 * fallback. No SQPOLL, no SINGLE_ISSUER, and no COOP_TASKRUN or
 * DEFER_TASKRUN: those require the submitter to enter the kernel to
 * flush completions, which fights the eventfd reaper.
 */
#ifndef GSH_IO_URING_H
#define GSH_IO_URING_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/uio.h>

struct gsh_uring_op {
	void (*complete)(struct gsh_uring_op *op, int res);
};

/**
 * Create this thread's ring on first use. queue_depth is rounded up to a
 * power of two, minimum 128. It also limits outstanding operations until
 * their completion callbacks return. Later calls keep the existing ring.
 *
 * @return 0 when the ring is ready, or a negative errno.
 */
int gsh_uring_ensure(unsigned queue_depth);

/** True on the reaper thread. That thread must not submit or wait. */
bool gsh_uring_in_reaper(void);

/**
 * Submit one read or write. complete() runs from the reaper, possibly
 * before this call returns. On failure the callback is not armed.
 * Stable writes synchronize data and all metadata (NFS FILE_SYNC).
 * Offsets must fit in a signed 64-bit file offset; UINT64_MAX is invalid.
 * Submission waits when the outstanding-operation limit is reached.
 *
 * @return 0 when the SQE was submitted, or a negative errno.
 */
int gsh_uring_submit_rw(int fd, bool is_write, void *buf, unsigned len,
			uint64_t offset, bool stable, struct gsh_uring_op *op);

/**
 * Submit one readv or writev. Same completion rules as gsh_uring_submit_rw().
 */
int gsh_uring_submit_rwv(int fd, bool is_write, const struct iovec *iov,
			 unsigned int nr_vecs, uint64_t offset, bool stable,
			 struct gsh_uring_op *op);

#endif
