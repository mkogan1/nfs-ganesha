/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file gsh_io_uring.h
 * @brief Shared io_uring owners for batched FSAL read/write.
 *
 * A bounded pool owns submission and completion, including the kernel enters
 * required by DEFER_TASKRUN. Each FSAL keeps its descriptor handling,
 * stable-write policy, and synchronous fallback.
 */
#ifndef GSH_IO_URING_H
#define GSH_IO_URING_H

#include <stdbool.h>
#include <stdint.h>
#include <sys/uio.h>

struct user_cred;

struct gsh_uring_op {
	void (*complete)(struct gsh_uring_op *op, int res);
	/* 0/1 keeps per-write RWF_SYNC; 2..32 allows a shared full fsync.
	 * These values are copied at submission. Delay is capped at 1000 us.
	 */
	unsigned int sync_batch_size;
	unsigned int sync_delay_us;
};

/**
 * Attach this thread to a ring owner on first use. Call with Ganesha's
 * credentials, before impersonating an NFS client. Up to four owners serve
 * all workers; each owner creates its own ring and reaps its own completions.
 * queue_depth is rounded up to a power of two, clamped to [128, 4096].
 * It limits this thread's outstanding operations until callbacks return.
 * Later calls retain this thread's limit. A new owner's ring uses the
 * attaching thread's depth; one entry is reserved for its wakeup poll.
 * Thread exit waits for this thread's callbacks; the last user joins the owner.
 *
 * @return 0 when the ring is ready, or a negative errno.
 */
int gsh_uring_ensure(unsigned int queue_depth);

/** True on the reaper thread. That thread must not submit or wait. */
bool gsh_uring_in_reaper(void);

/**
 * Queue one read or write. complete() runs from the owner, possibly
 * before this call returns. On failure the callback is not armed.
 * Stable writes synchronize data and all metadata (NFS FILE_SYNC).
 * Offsets must fit in a signed 64-bit file offset; UINT64_MAX is invalid.
 * Submission waits when this thread's outstanding-operation limit is reached.
 * creds is copied before returning (including supplementary groups); NULL
 * selects Ganesha's credentials. The caller need not impersonate the user.
 * Buffers, vectors, fd, and op must remain valid until complete() runs.
 * Requests with matching credentials are submitted in batches of up to 32;
 * a partial batch is submitted immediately unless sync_delay_us is enabled.
 * Group synchronization combines consecutive stable writes sharing the exact
 * fd, credentials and batching policy on this owner. All write completions
 * precede the shared full fsync; callbacks and credits are held through it.
 * A write error affects that write; a sync error fails all successful writes
 * in its group. Short writes retain their byte count after successful sync.
 * Singletons and allocation failures retain per-write RWF_SYNC. Collection may
 * overlap kernel I/O; completions and ready syncs are serviced immediately.
 * The first queued write sets the deadline, which wakeups never extend.
 *
 * @return 0 when queued (one callback guaranteed), or a negative errno.
 */
int gsh_uring_submit_rw(int fd, bool is_write, void *buf, unsigned int len,
			uint64_t offset, bool stable, struct gsh_uring_op *op,
			const struct user_cred *creds);

/**
 * Submit one readv or writev. Same completion rules as gsh_uring_submit_rw().
 */
int gsh_uring_submit_rwv(int fd, bool is_write, const struct iovec *iov,
			 unsigned int nr_vecs, uint64_t offset, bool stable,
			 struct gsh_uring_op *op,
			 const struct user_cred *creds);

#endif
