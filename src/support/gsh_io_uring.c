/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file gsh_io_uring.c
 * @brief Thread-local io_uring ring shared by FSAL modules.
 *
 * Compiled against liburing only when cmake is run with
 * -DWITH_SYSTEM_LIBURING=ON. Otherwise the calls return -ENOSYS
 * and each FSAL stays on its sync path.
 */
#include "config.h"
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/eventfd.h>

#ifdef USE_GSH_IO_URING
#include <liburing.h>
/* liburing.h defines UNUSED(x). Ganesha's UNUSED(...) is different. */
#undef UNUSED
#endif

#include "abstract_mem.h"
#include "gsh_io_uring.h"
#include "log.h"

#ifndef RWF_SYNC
#define RWF_SYNC 0x4
#endif

#ifdef USE_GSH_IO_URING

static __thread bool gsh_uring_tls_in_reaper;

struct gsh_uring_ring {
	struct io_uring ring;
	int event_fd;
	pthread_t reaper;
	bool initialized;
	bool shutdown;
	int init_error;
	unsigned completions;
	unsigned int inflight;
	pthread_mutex_t lock;
	pthread_cond_t cv;
};

static pthread_key_t gsh_uring_key;
static pthread_once_t gsh_uring_key_once = PTHREAD_ONCE_INIT;
static int gsh_uring_init_logged;

static unsigned gsh_uring_round_entries(unsigned requested)
{
	unsigned n = requested;

	if (n < 128)
		n = 128;
	if (n > 4096)
		n = 4096;
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	return n + 1;
}

static void gsh_uring_log_init_fail(int err, const char *what)
{
	if (__sync_bool_compare_and_swap(&gsh_uring_init_logged, 0, 1)) {
		LogWarn(COMPONENT_FSAL,
			"io_uring: %s failed: %s (%d); FSALs will use their sync path",
			what, strerror(err < 0 ? -err : err), err);
		if (err == -ENOSYS) {
			LogWarn(COMPONENT_FSAL,
				"io_uring: kernel returned ENOSYS; liburing is linked but this kernel has no io_uring syscall");
		}
	}
}

static void *gsh_uring_reaper(void *arg)
{
	struct gsh_uring_ring *st = arg;

	gsh_uring_tls_in_reaper = true;
#if defined(__linux__)
	pthread_setname_np(pthread_self(), "gsh-uring");
#endif
	while (true) {
		uint64_t val = 0;
		ssize_t n;
		struct io_uring_cqe *cqe;
		bool drained;

		n = read(st->event_fd, &val, sizeof(val));
		if (n < 0) {
			if (errno == EINTR)
				continue;
			LogWarn(COMPONENT_FSAL, "io_uring: eventfd read failed: %s",
				strerror(errno));
			if (errno == EBADF)
				break;
			continue;
		}
		while (io_uring_peek_cqe(&st->ring, &cqe) == 0) {
			struct gsh_uring_op *op = io_uring_cqe_get_data(cqe);
			int res = cqe->res;
			bool accounted = op != NULL;

			io_uring_cqe_seen(&st->ring, cqe);
			pthread_mutex_lock(&st->lock);
			st->completions++;
			pthread_cond_signal(&st->cv);
			pthread_mutex_unlock(&st->lock);
			if (op != NULL && op->complete != NULL)
				op->complete(op, res);
			if (accounted) {
				pthread_mutex_lock(&st->lock);
				st->inflight--;
				pthread_mutex_unlock(&st->lock);
			}
		}
		pthread_mutex_lock(&st->lock);
		drained = st->shutdown && st->inflight == 0;

		pthread_mutex_unlock(&st->lock);
		if (drained)
			break;
	}
	return NULL;
}

static void gsh_uring_ring_destroy(void *p)
{
	struct gsh_uring_ring *st = p;

	if (st == NULL)
		return;
	if (st->initialized) {
		uint64_t one = 1;
		ssize_t woke;

		/* A retiring worker may still own suspended NFS requests. */
		pthread_mutex_lock(&st->lock);
		st->shutdown = true;
		pthread_mutex_unlock(&st->lock);
		if (st->event_fd >= 0) {
			do {
				woke = write(st->event_fd, &one, sizeof(one));
			} while (woke < 0 && errno == EINTR);
			(void)woke;
		}
		pthread_join(st->reaper, NULL);
		io_uring_queue_exit(&st->ring);
	}
	if (st->event_fd >= 0)
		close(st->event_fd);
	if (st->initialized || st->init_error) {
		pthread_mutex_destroy(&st->lock);
		pthread_cond_destroy(&st->cv);
	}
	free(st);
}

static void gsh_uring_make_key(void)
{
	if (pthread_key_create(&gsh_uring_key, gsh_uring_ring_destroy) != 0)
		LogCrit(COMPONENT_FSAL, "io_uring: pthread_key_create failed");
}

static struct gsh_uring_ring *gsh_uring_state(void)
{
	pthread_once(&gsh_uring_key_once, gsh_uring_make_key);
	return pthread_getspecific(gsh_uring_key);
}

bool gsh_uring_in_reaper(void)
{
	return gsh_uring_tls_in_reaper;
}

int gsh_uring_ensure(unsigned queue_depth)
{
	struct gsh_uring_ring *st;
	struct io_uring_params params;
	unsigned entries;
	int ret;

	if (gsh_uring_tls_in_reaper)
		return -EAGAIN;

	st = gsh_uring_state();
	if (st && st->initialized)
		return 0;
	if (st && st->init_error)
		return st->init_error;

	entries = gsh_uring_round_entries(queue_depth);
	if (st == NULL) {
		st = calloc(1, sizeof(*st));
		if (st == NULL) {
			gsh_uring_log_init_fail(-ENOMEM, "ring alloc");
			return -ENOMEM;
		}
		st->event_fd = -1;
		pthread_mutex_init(&st->lock, NULL);
		pthread_cond_init(&st->cv, NULL);
		if (pthread_setspecific(gsh_uring_key, st) != 0) {
			gsh_uring_log_init_fail(-errno, "pthread_setspecific");
			pthread_mutex_destroy(&st->lock);
			pthread_cond_destroy(&st->cv);
			free(st);
			return -errno;
		}
	}

	memset(&params, 0, sizeof(params));
	params.flags = 0;
	ret = io_uring_queue_init_params(entries, &st->ring, &params);
	if (ret < 0) {
		st->init_error = ret;
		gsh_uring_log_init_fail(ret, "io_uring_queue_init_params");
		return ret;
	}
	st->event_fd = eventfd(0, EFD_CLOEXEC);
	if (st->event_fd < 0) {
		ret = -errno;
		io_uring_queue_exit(&st->ring);
		st->init_error = ret;
		gsh_uring_log_init_fail(ret, "eventfd");
		return ret;
	}
	ret = io_uring_register_eventfd(&st->ring, st->event_fd);
	if (ret < 0) {
		close(st->event_fd);
		st->event_fd = -1;
		io_uring_queue_exit(&st->ring);
		st->init_error = ret;
		gsh_uring_log_init_fail(ret, "io_uring_register_eventfd");
		return ret;
	}
	st->shutdown = false;
	ret = pthread_create(&st->reaper, NULL, gsh_uring_reaper, st);
	if (ret != 0) {
		close(st->event_fd);
		st->event_fd = -1;
		io_uring_queue_exit(&st->ring);
		st->init_error = -ret;
		gsh_uring_log_init_fail(-ret, "reaper pthread_create");
		return -ret;
	}
	st->initialized = true;
	LogEvent(COMPONENT_FSAL, "io_uring: ring ready entries=%u flags=0x%x",
		 entries, params.flags);
	return 0;
}

static struct gsh_uring_ring *gsh_uring_ready(void)
{
	struct gsh_uring_ring *st = gsh_uring_state();

	if (st == NULL || !st->initialized)
		return NULL;
	return st;
}

static int gsh_uring_submit_pending(struct gsh_uring_ring *st)
{
	int total = 0;
	int spins = 0;

	while (io_uring_sq_ready(&st->ring) > 0) {
		int n = io_uring_submit(&st->ring);

		if (n > 0) {
			total += n;
			spins = 0;
			continue;
		}
		if (n == 0 || n == -EAGAIN || n == -EBUSY) {
			if (++spins >= 16)
				return total > 0 ? total : (n < 0 ? n : -EAGAIN);
			continue;
		}
		return total > 0 ? total : n;
	}
	return total;
}

static bool gsh_uring_wait_space(struct gsh_uring_ring *st)
{
	unsigned start;
	bool ready;

	if (gsh_uring_tls_in_reaper)
		return false;
	pthread_mutex_lock(&st->lock);
	start = st->completions;
	while (st->completions == start && !st->shutdown)
		pthread_cond_wait(&st->cv, &st->lock);
	ready = !st->shutdown;
	pthread_mutex_unlock(&st->lock);
	return ready;
}

static struct io_uring_sqe *gsh_uring_get_sqe(struct gsh_uring_ring *st)
{
	for (;;) {
		struct io_uring_sqe *sqe;

		sqe = io_uring_get_sqe(&st->ring);
		if (sqe)
			return sqe;
		(void)gsh_uring_submit_pending(st);
		sqe = io_uring_get_sqe(&st->ring);
		if (sqe)
			return sqe;
		if (!gsh_uring_wait_space(st))
			return NULL;
	}
}

static int gsh_uring_arm(struct gsh_uring_ring *st, struct io_uring_sqe *sqe,
			 struct gsh_uring_op *op)
{
	/* Count before submitting: the CQE may arrive before submit returns. */
	pthread_mutex_lock(&st->lock);
	st->inflight++;
	pthread_mutex_unlock(&st->lock);
	io_uring_sqe_set_data(sqe, op);
	(void)gsh_uring_submit_pending(st);
	if (io_uring_sq_ready(&st->ring) != 0) {
		/* Not consumed. A later submit must not run this callback. */
		io_uring_prep_nop(sqe);
		io_uring_sqe_set_data(sqe, NULL);
		pthread_mutex_lock(&st->lock);
		st->inflight--;
		pthread_mutex_unlock(&st->lock);
		return -EAGAIN;
	}
	return 0;
}

int gsh_uring_submit_rw(int fd, bool is_write, void *buf, unsigned len,
			uint64_t offset, bool stable, struct gsh_uring_op *op)
{
	struct gsh_uring_ring *st;
	struct io_uring_sqe *sqe;

	if (gsh_uring_tls_in_reaper)
		return -EAGAIN;
	/* UINT64_MAX means current file position to io_uring, not an offset. */
	if (offset > INT64_MAX || op == NULL || op->complete == NULL)
		return -EINVAL;
	st = gsh_uring_ready();
	if (st == NULL)
		return -EIO;
	sqe = gsh_uring_get_sqe(st);
	if (sqe == NULL)
		return -EAGAIN;
	if (is_write)
		io_uring_prep_write(sqe, fd, buf, len, offset);
	else
		io_uring_prep_read(sqe, fd, buf, len, offset);
	if (is_write && stable)
		sqe->rw_flags = RWF_SYNC;
	return gsh_uring_arm(st, sqe, op);
}

int gsh_uring_submit_rwv(int fd, bool is_write, const struct iovec *iov,
			 unsigned int nr_vecs, uint64_t offset, bool stable,
			 struct gsh_uring_op *op)
{
	struct gsh_uring_ring *st;
	struct io_uring_sqe *sqe;
	struct iovec *iov_rw = (struct iovec *)iov;

	if (gsh_uring_tls_in_reaper)
		return -EAGAIN;
	if (iov == NULL || nr_vecs == 0 || offset > INT64_MAX ||
	    op == NULL || op->complete == NULL)
		return -EINVAL;
	st = gsh_uring_ready();
	if (st == NULL)
		return -EIO;
	sqe = gsh_uring_get_sqe(st);
	if (sqe == NULL)
		return -EAGAIN;
	if (is_write)
		io_uring_prep_writev(sqe, fd, iov_rw, nr_vecs, offset);
	else
		io_uring_prep_readv(sqe, fd, iov_rw, nr_vecs, offset);
	if (is_write && stable)
		sqe->rw_flags = RWF_SYNC;
	return gsh_uring_arm(st, sqe, op);
}

#else /* !USE_GSH_IO_URING */

int gsh_uring_ensure(unsigned queue_depth)
{
	(void)queue_depth;
	return -ENOSYS;
}

bool gsh_uring_in_reaper(void)
{
	return false;
}

int gsh_uring_submit_rw(int fd, bool is_write, void *buf, unsigned len,
			uint64_t offset, bool stable, struct gsh_uring_op *op)
{
	(void)fd;
	(void)is_write;
	(void)buf;
	(void)len;
	(void)offset;
	(void)stable;
	(void)op;
	return -ENOSYS;
}

int gsh_uring_submit_rwv(int fd, bool is_write, const struct iovec *iov,
			 unsigned int nr_vecs, uint64_t offset, bool stable,
			 struct gsh_uring_op *op)
{
	(void)fd;
	(void)is_write;
	(void)iov;
	(void)nr_vecs;
	(void)offset;
	(void)stable;
	(void)op;
	return -ENOSYS;
}

#endif /* USE_GSH_IO_URING */
