/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file gsh_io_uring.c
 * @brief Batched FSAL I/O with one issuer/completion owner per ring.
 */
#include "config.h"
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <poll.h>
#include <time.h>

#ifdef USE_GSH_IO_URING
#include <liburing.h>
#undef UNUSED
#endif

#include "gsh_io_uring.h"
#include "log.h"
#ifdef USE_GSH_IO_URING
#include "FSAL/access_check.h"
#endif

#ifndef RWF_SYNC
#define RWF_SYNC 0x4
#endif

#ifdef USE_GSH_IO_URING

#define GSH_URING_BATCH 32
#define GSH_URING_OWNERS 4
#define GSH_URING_WAKE 1ULL

struct gsh_uring_client;
struct gsh_uring_sync;
struct gsh_uring_request {
	struct gsh_uring_request *next;
	struct gsh_uring_client *client;
	struct gsh_uring_op *op;
	int fd;
	bool write, vector, stable, user, sync_phase;
	unsigned int sync_batch_size, sync_delay_us;
	struct timespec collect_until;
	struct gsh_uring_sync *sync;
	int result;
	void *buf;
	unsigned int len;
	uint64_t offset;
	struct user_cred creds;
	gid_t groups[];
};

/* A closed cohort: no writes can join after its SQEs are prepared. */
struct gsh_uring_sync {
	struct gsh_uring_sync *next;
	unsigned int count, pending;
	struct gsh_uring_request *writes[GSH_URING_BATCH];
};

struct gsh_uring_ring {
	struct io_uring ring;
	int event_fd;
	pthread_t owner;
	pthread_mutex_t lock;
	pthread_cond_t cv;
	bool ready, shutdown, waiting, collecting;
	int init_error, submit_error;
	unsigned int entries, active;
	unsigned int collect_count; /* Protected by lock, like collecting. */
	struct gsh_uring_sync *sync_head, *sync_tail;
	uint64_t sync_groups, grouped_writes, single_stable, sync_errors;
	uint64_t next_sync_report;
	struct user_cred daemon_creds;
	/* Protected by pool_lock; clients keep the ring alive. */
	unsigned int users;
	/* Protected by lock. The owner alone touches SQ/CQ and active. */
	struct gsh_uring_request *head, *tail;
};

struct gsh_uring_client {
	struct gsh_uring_ring *ring;
	unsigned int max_inflight, inflight;
	int init_error;
};

static __thread bool gsh_uring_tls_in_reaper;
static pthread_key_t gsh_uring_key;
static pthread_once_t gsh_uring_key_once = PTHREAD_ONCE_INIT;
static int gsh_uring_key_error;
static int gsh_uring_init_logged;
static pthread_mutex_t pool_lock = PTHREAD_MUTEX_INITIALIZER;
static struct gsh_uring_ring *pool[GSH_URING_OWNERS];

static unsigned int gsh_uring_round_entries(unsigned int requested)
{
	unsigned int n = requested;

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
	if (__sync_bool_compare_and_swap(&gsh_uring_init_logged, 0, 1))
		LogWarn(COMPONENT_FSAL,
			"io_uring: %s failed: %s (%d); using sync I/O", what,
			strerror(-err), err);
}

static void gsh_uring_wake(struct gsh_uring_ring *st)
{
	uint64_t one = 1;
	ssize_t n;

	do {
		n = write(st->event_fd, &one, sizeof(one));
	} while (n < 0 && errno == EINTR);
	/* A saturated eventfd is already readable. */
	if (n != sizeof(one) && !(n < 0 && errno == EAGAIN))
		LogFatal(COMPONENT_FSAL, "io_uring: owner wake failed");
}

static void gsh_uring_drain_wake(struct gsh_uring_ring *st)
{
	uint64_t value;
	ssize_t n;

	do {
		n = read(st->event_fd, &value, sizeof(value));
	} while (n == sizeof(value) || (n < 0 && errno == EINTR));
	if (n >= 0 || errno != EAGAIN)
		LogFatal(COMPONENT_FSAL, "io_uring: owner wake read failed");
}

static bool gsh_uring_same_creds(const struct user_cred *a,
				 const struct user_cred *b)
{
	return a->caller_uid == b->caller_uid &&
	       a->caller_gid == b->caller_gid &&
	       a->caller_glen == b->caller_glen &&
	       (a->caller_glen == 0 ||
		memcmp(a->caller_garray, b->caller_garray,
		       a->caller_glen * sizeof(gid_t)) == 0);
}

static bool gsh_uring_same_user(const struct gsh_uring_request *a,
				const struct gsh_uring_request *b)
{
	return a->user == b->user &&
	       (!a->user || gsh_uring_same_creds(&a->creds, &b->creds));
}

static void gsh_uring_complete(struct gsh_uring_request *req, int res)
{
	struct gsh_uring_client *client = req->client;
	struct gsh_uring_ring *st = client->ring;

	/*
	 * The callback may free op. Retain the credit and client until it
	 * returns.
	 */
	req->op->complete(req->op, res);
	free(req);
	pthread_mutex_lock(&st->lock);
	client->inflight--;
	pthread_cond_broadcast(&st->cv);
	pthread_mutex_unlock(&st->lock);
}

/* Save clients before callbacks: callbacks may free their op and release fd. */
static void gsh_uring_complete_many(struct gsh_uring_request **requests,
				    unsigned int n)
{
	struct gsh_uring_client *clients[GSH_URING_BATCH];
	struct gsh_uring_ring *st = requests[0]->client->ring;
	unsigned int i;

	for (i = 0; i < n; i++) {
		struct gsh_uring_request *req = requests[i];

		clients[i] = req->client;
		req->op->complete(req->op, req->result);
		free(req);
	}
	pthread_mutex_lock(&st->lock);
	for (i = 0; i < n; i++)
		clients[i]->inflight--;
	pthread_cond_broadcast(&st->cv);
	pthread_mutex_unlock(&st->lock);
}

static void gsh_uring_sync_complete(struct gsh_uring_ring *st,
				    struct gsh_uring_sync *sync, int res)
{
	unsigned int i;

	if (res < 0)
		st->sync_errors++;
	for (i = 0; i < sync->count; i++)
		if (res < 0 && sync->writes[i]->result >= 0)
			sync->writes[i]->result = res;
	gsh_uring_complete_many(sync->writes, sync->count);
	free(sync);
}

/* No callback runs at a grouped write CQE, including failed/short writes. */
static void gsh_uring_sync_write_done(struct gsh_uring_ring *st,
				      struct gsh_uring_request *req, int res)
{
	struct gsh_uring_sync *sync = req->sync;

	req->result = res;
	if (--sync->pending != 0)
		return;
	/* The fsync is queued only AFTER every write CQE, never merely after
	 * the write SQEs. Submission order alone is not an I/O dependency.
	 */
	if (st->sync_tail)
		st->sync_tail->next = sync;
	else
		st->sync_head = sync;
	st->sync_tail = sync;
}

static unsigned int gsh_uring_reap(struct gsh_uring_ring *st, bool *poll_armed)
{
	struct io_uring_cqe *cqes[GSH_URING_BATCH];
	struct gsh_uring_request *completed[GSH_URING_BATCH];
	struct {
		uint64_t data;
		int res;
	} batch[GSH_URING_BATCH];
	unsigned int n, i, count = 0;

	n = io_uring_peek_batch_cqe(&st->ring, cqes, GSH_URING_BATCH);
	for (i = 0; i < n; i++) {
		batch[i].data = cqes[i]->user_data;
		batch[i].res = cqes[i]->res;
	}
	io_uring_cq_advance(&st->ring, n);
	for (i = 0; i < n; i++) {
		if (batch[i].data == GSH_URING_WAKE) {
			if (batch[i].res < 0)
				LogFatal(COMPONENT_FSAL,
					 "io_uring: wake poll failed: %d",
					 batch[i].res);
			gsh_uring_drain_wake(st);
			*poll_armed = false;
		} else if (batch[i].data != 0) {
			struct gsh_uring_request *req =
				(void *)(uintptr_t)batch[i].data;

			st->active--;
			if (req->sync_phase)
				gsh_uring_sync_complete(st, req->sync,
							batch[i].res);
			else if (req->sync)
				gsh_uring_sync_write_done(st, req,
							  batch[i].res);
			else {
				req->result = batch[i].res;
				completed[count++] = req;
			}
		}
	}
	if (count)
		gsh_uring_complete_many(completed, count);
	return n;
}

static bool gsh_uring_can_group(const struct gsh_uring_request *a,
				const struct gsh_uring_request *b)
{
	return a->write && a->stable && a->sync_batch_size > 1 && b->write &&
	       b->stable && a->fd == b->fd &&
	       a->sync_batch_size == b->sync_batch_size &&
	       a->sync_delay_us == b->sync_delay_us &&
	       gsh_uring_same_user(a, b);
}

/* Recompute the remaining wait: completions and arrivals never reset it. */
static bool gsh_uring_remaining(const struct timespec *until,
				struct timespec *remaining)
{
	struct timespec now;

	clock_gettime(CLOCK_MONOTONIC, &now);
	remaining->tv_sec = until->tv_sec - now.tv_sec;
	remaining->tv_nsec = until->tv_nsec - now.tv_nsec;
	if (remaining->tv_nsec < 0) {
		remaining->tv_sec--;
		remaining->tv_nsec += 1000000000;
	}
	return remaining->tv_sec > 0 ||
	       (remaining->tv_sec == 0 && remaining->tv_nsec > 0);
}

/* Called with the producer lock held. Keep a partial compatible batch queued
 * while the owner continues to reap CQEs and submit ready syncs. The first
 * request's enqueue deadline bounds collection, even across those completions.
 */
static bool gsh_uring_collect(struct gsh_uring_ring *st,
			      struct timespec *until)
{
	struct gsh_uring_request *first = st->head, *req;
	struct timespec remaining;
	unsigned int n;

	if (!first || !gsh_uring_can_group(first, first) ||
	    !first->sync_delay_us || st->sync_head ||
	    st->active >= st->entries - 1 || st->shutdown || st->submit_error)
		return false;
	for (n = 0, req = first; req && n < first->sync_batch_size;
	     req = req->next, n++)
		if (!gsh_uring_can_group(first, req))
			return false;
	if (n == first->sync_batch_size)
		return false;
	*until = first->collect_until;
	st->collect_count = n;
	return gsh_uring_remaining(until, &remaining);
}

/* The wake poll is submitted before this wait. Kernel completions interrupt
 * collection, including deferred task work that requires GETEVENTS. Do not
 * consume the CQE here: the normal reaper owns callbacks and credit release.
 */
static int gsh_uring_wait_collect(struct gsh_uring_ring *st,
				 const struct timespec *until)
{
	struct timespec remaining;
	int ret;

	if (!gsh_uring_remaining(until, &remaining))
		return 0;
#ifdef IORING_FEAT_EXT_ARG
	if (st->ring.features & IORING_FEAT_EXT_ARG) {
		struct __kernel_timespec ts = {
			.tv_sec = remaining.tv_sec,
			.tv_nsec = remaining.tv_nsec,
		};
		struct io_uring_cqe *cqe;

		ret = io_uring_wait_cqe_timeout(&st->ring, &cqe, &ts);
	} else
#endif
	{
		/* Older kernels have no DEFER_TASKRUN. Polling both descriptors
		 * avoids liburing's timeout-SQE fallback and its extra SQ slot.
		 */
		struct pollfd fds[] = {
			{ .fd = st->ring.ring_fd, .events = POLLIN },
			{ .fd = st->event_fd, .events = POLLIN },
		};

		ret = ppoll(fds, 2, &remaining, NULL);
		if (ret < 0)
			ret = -errno;
		else if ((fds[0].revents | fds[1].revents) & POLLNVAL)
			ret = -EBADF;
		else if ((fds[0].revents | fds[1].revents) &
			 (POLLERR | POLLHUP))
			ret = -EIO;
	}
	return ret >= 0 || ret == -ETIME || ret == -EINTR ? 0 : ret;
}

static void gsh_uring_group_batch(struct gsh_uring_ring *st,
				  struct gsh_uring_request **batch,
				  unsigned int n)
{
	unsigned int i = 0, j, count;

	while (i < n) {
		struct gsh_uring_sync *sync;

		count = 1;
		while (i + count < n && count < batch[i]->sync_batch_size &&
		       gsh_uring_can_group(batch[i], batch[i + count]))
			count++;
		/*
		 * Keep the one-SQE RWF_SYNC path for singletons and low memory.
		 */
		sync = count > 1 ? calloc(1, sizeof(*sync)) : NULL;
		if (sync) {
			sync->count = sync->pending = count;
			for (j = 0; j < count; j++) {
				sync->writes[j] = batch[i + j];
				batch[i + j]->sync = sync;
			}
			st->sync_groups++;
			st->grouped_writes += count;
		} else {
			for (j = 0; j < count; j++)
				if (batch[i + j]->write && batch[i + j]->stable)
					st->single_stable++;
		}
		i += count;
	}
	if (st->sync_groups &&
	    st->grouped_writes + st->single_stable >= st->next_sync_report) {
		LogEvent(COMPONENT_FSAL,
			 "io_uring sync totals: groups=%" PRIu64
			 " grouped_writes=%" PRIu64 " single_stable=%" PRIu64
			 " sync_errors=%" PRIu64,
			 st->sync_groups, st->grouped_writes, st->single_stable,
			 st->sync_errors);
		st->next_sync_report =
			st->grouped_writes + st->single_stable + 65536;
	}
}

static void gsh_uring_prep(struct io_uring_sqe *sqe,
			   struct gsh_uring_request *req)
{
	if (req->sync_phase) {
		io_uring_prep_fsync(sqe, req->fd, 0);
		io_uring_sqe_set_data(sqe, req);
		return;
	}
	if (req->vector) {
		if (req->write)
			io_uring_prep_writev(sqe, req->fd, req->buf, req->len,
					     req->offset);
		else
			io_uring_prep_readv(sqe, req->fd, req->buf, req->len,
					    req->offset);
	} else if (req->write) {
		io_uring_prep_write(sqe, req->fd, req->buf, req->len,
				    req->offset);
	} else {
		io_uring_prep_read(sqe, req->fd, req->buf, req->len,
				   req->offset);
	}
	if (req->write && req->stable && !req->sync)
		sqe->rw_flags = RWF_SYNC;
	io_uring_sqe_set_data(sqe, req);
}

/* All pending SQEs have the same credentials. No SQPOLL consumes them. */
static int gsh_uring_submit_pending(struct gsh_uring_ring *st,
				    unsigned int wait_nr)
{
	unsigned int retries = 0;
	int ret;

	do {
		/*
		 * GETEVENTS drives DEFER_TASKRUN, even with no new submissions.
		 */
		ret = io_uring_submit_and_wait(&st->ring, wait_nr);
		if (io_uring_sq_ready(&st->ring) == 0 &&
		    (ret >= 0 || ret == -EINTR))
			return 0;
		if (ret > 0) {
			retries = 0;
			continue;
		}
		if (ret == 0 || ret == -EINTR || ret == -EAGAIN ||
		    ret == -EBUSY) {
			if (++retries < 16)
				continue;
		}
		return ret < 0 ? ret : -EAGAIN;
	} while (io_uring_sq_ready(&st->ring) != 0);
	return 0;
}

static int gsh_uring_setup(struct gsh_uring_ring *st)
{
	struct io_uring_params p = { 0 };
	int ret;

#if defined(IORING_SETUP_SINGLE_ISSUER) &&     \
	defined(IORING_SETUP_DEFER_TASKRUN) && \
	defined(IORING_SETUP_COOP_TASKRUN)
	p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
		  IORING_SETUP_COOP_TASKRUN;
#endif
	ret = io_uring_queue_init_params(st->entries, &st->ring, &p);
	if (ret == -EINVAL && p.flags != 0) {
		memset(&p, 0, sizeof(p));
		ret = io_uring_queue_init_params(st->entries, &st->ring, &p);
	}
	if (ret == 0) {
#ifdef IORING_SETUP_DEFER_TASKRUN
		/*
		 * Headers with DEFER_TASKRUN also provide the worker-limit API.
		 */
		unsigned int workers[2] = { 8, 0 };

		/* Sharing rings must not multiply the bounded worker pool. */
		int limit_ret =
			io_uring_register_iowq_max_workers(&st->ring, workers);

		if (limit_ret < 0)
			LogDebug(COMPONENT_FSAL,
				 "io_uring: worker limit unavailable: %d",
				 limit_ret);
#endif
		LogEvent(COMPONENT_FSAL,
			 "io_uring: owner ready entries=%u flags=0x%x batch=%u",
			 st->entries, p.flags, GSH_URING_BATCH);
	}
	return ret;
}

static int gsh_uring_save_creds(struct gsh_uring_ring *st)
{
	int count = getgroups(0, NULL);

	if (count < 0)
		return -errno;
	st->daemon_creds.caller_uid = geteuid();
	st->daemon_creds.caller_gid = getegid();
	st->daemon_creds.caller_glen = count;
	if (count) {
		st->daemon_creds.caller_garray = malloc(count * sizeof(gid_t));
		if (!st->daemon_creds.caller_garray)
			return -ENOMEM;
		if (getgroups(count, st->daemon_creds.caller_garray) < 0)
			return -errno;
	}
	return 0;
}

static void gsh_uring_fail_batch(struct gsh_uring_ring *st,
				 struct gsh_uring_request **batch,
				 struct io_uring_sqe **sqes, unsigned int n,
				 int ret)
{
	unsigned int i, left = io_uring_sq_ready(&st->ring);

	/* Partial submissions consume a prefix. Disarm only the suffix. */
	if (left > n)
		left = n;
	for (i = n - left; i < n; i++) {
		memset(sqes[i], 0, sizeof(*sqes[i]));
		io_uring_prep_nop(sqes[i]);
		io_uring_sqe_set_data64(sqes[i], 0);
		st->active--;
		if (batch[i]->sync_phase)
			gsh_uring_sync_complete(st, batch[i]->sync, ret);
		else if (batch[i]->sync)
			gsh_uring_sync_write_done(st, batch[i], ret);
		else
			gsh_uring_complete(batch[i], ret);
	}
}

static void *gsh_uring_owner(void *arg)
{
	struct gsh_uring_ring *st = arg;
	struct gsh_uring_request *batch[GSH_URING_BATCH];
	struct io_uring_sqe *sqes[GSH_URING_BATCH];
	bool poll_armed = false;
	int ret;

	gsh_uring_tls_in_reaper = true;
	pthread_setname_np(pthread_self(), "gsh-uring");
	/* SINGLE_ISSUER binds the ring to the thread that creates it. */
	ret = gsh_uring_save_creds(st);
	if (ret == 0)
		ret = gsh_uring_setup(st);
	pthread_mutex_lock(&st->lock);
	st->init_error = ret;
	st->ready = true;
	pthread_cond_broadcast(&st->cv);
	pthread_mutex_unlock(&st->lock);
	if (ret < 0)
		return NULL;

	for (;;) {
		unsigned int n = 0, i;
		struct io_uring_sqe *sqe;
		bool shutdown, impersonate, syncing, collecting;
		struct timespec collect_until = { 0 };
		unsigned int wait_nr;

		gsh_uring_reap(st, &poll_armed);
		pthread_mutex_lock(&st->lock);
		collecting = gsh_uring_collect(st, &collect_until);
		st->collecting = collecting;
		shutdown = st->shutdown;
		syncing = st->sync_head != NULL;
		/*
		 * Prioritize closed cohorts so callbacks can release their
		 * credits. A single submit uses one credential set, including
		 * for fsync. Leave a slot for the poll that wakes an idle
		 * owner.
		 */
		while (!collecting && n < GSH_URING_BATCH &&
		       st->active + n < st->entries - 1) {
			struct gsh_uring_request *req;

			if (syncing) {
				struct gsh_uring_sync *sync = st->sync_head;

				if (!sync)
					break;
				req = sync->writes[0];
				if (n && !gsh_uring_same_user(batch[0], req))
					break;
				st->sync_head = sync->next;
				if (!st->sync_head)
					st->sync_tail = NULL;
				req->sync_phase = true;
			} else {
				req = st->head;
				if (!req ||
				    (n && !gsh_uring_same_user(batch[0], req)))
					break;
				st->head = req->next;
			}
			batch[n++] = req;
		}
		if (st->head == NULL)
			st->tail = NULL;
		/*
		 * Only a parked owner needs an eventfd notification. A busy
		 * owner comes back to this queue after its non-waiting submit.
		 * Publish the decision under the producer lock to avoid a lost
		 * wakeup.
		 */
		wait_nr = !collecting &&
			  (n == 0 || st->active + n == st->entries - 1);
		st->waiting = collecting || wait_nr != 0;
		pthread_mutex_unlock(&st->lock);
		if (shutdown && st->active == 0 && n == 0)
			break;
		if (st->submit_error) {
			for (i = 0; i < n; i++) {
				if (syncing)
					gsh_uring_sync_complete(
						st, batch[i]->sync,
						st->submit_error);
				else
					gsh_uring_complete(batch[i],
							   st->submit_error);
			}
			if (n)
				continue;
			if (st->active == 0)
				break;
		}
		if (!syncing)
			gsh_uring_group_batch(st, batch, n);
		if (!poll_armed) {
			sqe = io_uring_get_sqe(&st->ring);
			if (sqe == NULL)
				LogFatal(COMPONENT_FSAL,
					 "io_uring: no wake SQE");
			io_uring_prep_poll_add(sqe, st->event_fd, POLLIN);
			io_uring_sqe_set_data64(sqe, GSH_URING_WAKE);
			poll_armed = true;
		}
		for (i = 0; i < n; i++) {
			sqes[i] = io_uring_get_sqe(&st->ring);
			if (sqes[i] == NULL)
				LogFatal(COMPONENT_FSAL,
					 "io_uring: no I/O SQE");
			gsh_uring_prep(sqes[i], batch[i]);
		}
		st->active += n;
		impersonate = n && batch[0]->user &&
			      !gsh_uring_same_creds(&batch[0]->creds,
						    &st->daemon_creds);
		if (impersonate)
			fsal_set_credentials(&batch[0]->creds);
		ret = gsh_uring_submit_pending(st, wait_nr);
		if (impersonate) {
			/* Regain the daemon uid before restoring groups. */
			if (syscall(SYS_setresuid, -1,
				    st->daemon_creds.caller_uid, -1) != 0)
				LogFatal(COMPONENT_FSAL,
					 "io_uring: cannot restore daemon uid");
			fsal_set_credentials(&st->daemon_creds);
		}
		if (ret == 0 && collecting)
			ret = gsh_uring_wait_collect(st, &collect_until);
		if (ret == 0)
			continue;
		/*
		 * A second failure while draining accepted I/O cannot be
		 * ignored.
		 */
		if (st->submit_error)
			LogFatal(COMPONENT_FSAL,
				 "io_uring: cannot drain failed ring: %d", ret);
		pthread_mutex_lock(&st->lock);
		st->submit_error = ret;
		pthread_cond_broadcast(&st->cv);
		pthread_mutex_unlock(&st->lock);
		LogWarn(COMPONENT_FSAL,
			"io_uring: submit failed: %d; retiring owner", ret);
		gsh_uring_fail_batch(st, batch, sqes, n, ret);
	}
	io_uring_queue_exit(&st->ring);
	return NULL;
}

static void gsh_uring_destroy(struct gsh_uring_ring *st)
{
	int ret = pthread_join(st->owner, NULL);

	if (ret != 0)
		LogFatal(COMPONENT_FSAL, "io_uring: owner join failed: %s",
			 strerror(ret));
	close(st->event_fd);
	pthread_cond_destroy(&st->cv);
	pthread_mutex_destroy(&st->lock);
	free(st->daemon_creds.caller_garray);
	free(st);
}

static void gsh_uring_client_destroy(void *arg)
{
	struct gsh_uring_client *client = arg;
	struct gsh_uring_ring *st = client->ring;
	bool last;
	unsigned int i;

	if (st == NULL) {
		free(client);
		return;
	}
	pthread_mutex_lock(&st->lock);
	while (client->inflight != 0)
		pthread_cond_wait(&st->cv, &st->lock);
	pthread_mutex_unlock(&st->lock);
	pthread_mutex_lock(&pool_lock);
	last = --st->users == 0;
	if (last) {
		for (i = 0; i < GSH_URING_OWNERS; i++)
			if (pool[i] == st)
				pool[i] = NULL;
		pthread_mutex_lock(&st->lock);
		st->shutdown = true;
		gsh_uring_wake(st);
		pthread_mutex_unlock(&st->lock);
	}
	pthread_mutex_unlock(&pool_lock);
	if (last)
		gsh_uring_destroy(st);
	free(client);
}

static void gsh_uring_make_key(void)
{
	gsh_uring_key_error =
		pthread_key_create(&gsh_uring_key, gsh_uring_client_destroy);
}

static struct gsh_uring_client *gsh_uring_state(void)
{
	pthread_once(&gsh_uring_key_once, gsh_uring_make_key);
	return gsh_uring_key_error ? NULL : pthread_getspecific(gsh_uring_key);
}

static int gsh_uring_create(unsigned int entries,
			    struct gsh_uring_ring **result)
{
	struct gsh_uring_ring *st = calloc(1, sizeof(*st));
	pthread_condattr_t attr;
	int ret;

	if (!st)
		return -ENOMEM;
	st->entries = entries;
	st->next_sync_report = 65536;
	ret = pthread_mutex_init(&st->lock, NULL);
	if (ret)
		goto free_st;
	ret = pthread_condattr_init(&attr);
	if (ret)
		goto destroy_lock;
	ret = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	if (ret == 0)
		ret = pthread_cond_init(&st->cv, &attr);
	pthread_condattr_destroy(&attr);
	if (ret)
		goto destroy_lock;
	st->event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (st->event_fd < 0) {
		ret = errno;
		goto destroy_cv;
	}
	ret = pthread_create(&st->owner, NULL, gsh_uring_owner, st);
	if (ret)
		goto close_fd;
	pthread_mutex_lock(&st->lock);
	while (!st->ready)
		pthread_cond_wait(&st->cv, &st->lock);
	ret = st->init_error;
	pthread_mutex_unlock(&st->lock);
	if (ret) {
		gsh_uring_destroy(st);
		return ret;
	}
	*result = st;
	return 0;
close_fd:
	close(st->event_fd);
destroy_cv:
	pthread_cond_destroy(&st->cv);
destroy_lock:
	pthread_mutex_destroy(&st->lock);
free_st:
	free(st);
	return -ret;
}

bool gsh_uring_in_reaper(void)
{
	return gsh_uring_tls_in_reaper;
}

int gsh_uring_ensure(unsigned int queue_depth)
{
	struct gsh_uring_client *client;
	struct gsh_uring_ring *st = NULL;
	unsigned int i, slot = GSH_URING_OWNERS;
	int ret = 0;

	if (gsh_uring_tls_in_reaper)
		return -EAGAIN;
	client = gsh_uring_state();
	if (gsh_uring_key_error)
		return -gsh_uring_key_error;
	if (client)
		return client->init_error;
	client = calloc(1, sizeof(*client));
	if (!client)
		return -ENOMEM;
	ret = pthread_setspecific(gsh_uring_key, client);
	if (ret) {
		free(client);
		return -ret;
	}
	client->max_inflight = gsh_uring_round_entries(queue_depth);
	pthread_mutex_lock(&pool_lock);
	for (i = 0; i < GSH_URING_OWNERS; i++) {
		if (!pool[i]) {
			slot = i;
			break;
		}
		if (!st || pool[i]->users < st->users)
			st = pool[i];
	}
	if (slot < GSH_URING_OWNERS) {
		ret = gsh_uring_create(client->max_inflight, &st);
		if (ret)
			goto unlock;
		pool[slot] = st;
	}
	st->users++;
	client->ring = st;
unlock:
	pthread_mutex_unlock(&pool_lock);
	if (ret) {
		gsh_uring_log_init_fail(ret, "owner creation");
		/*
		 * Do not create a failing owner on every I/O on unsupported
		 * hosts.
		 */
		if (ret == -ENOSYS || ret == -EPERM || ret == -EACCES ||
		    ret == -EOPNOTSUPP || ret == -EINVAL) {
			client->init_error = ret;
			return ret;
		}
		pthread_setspecific(gsh_uring_key, NULL);
		free(client);
		return ret;
	}
	return 0;
}

static int gsh_uring_queue(int fd, bool write, bool vector, void *buf,
			   unsigned int len, uint64_t offset, bool stable,
			   struct gsh_uring_op *op,
			   const struct user_cred *creds)
{
	struct gsh_uring_client *client;
	struct gsh_uring_ring *st;
	struct gsh_uring_request *req;
	size_t groups = creds ? creds->caller_glen : 0;
	bool wake;
	int ret;

	if (gsh_uring_tls_in_reaper)
		return -EAGAIN;
	if (offset > INT64_MAX || !op || !op->complete ||
	    op->sync_batch_size > GSH_URING_BATCH || op->sync_delay_us > 1000 ||
	    (vector && (!buf || !len)) || (groups && !creds->caller_garray) ||
	    groups > (SIZE_MAX - sizeof(*req)) / sizeof(gid_t))
		return -EINVAL;
	client = gsh_uring_state();
	if (!client)
		return -EIO;
	if (client->init_error)
		return client->init_error;
	st = client->ring;
	req = malloc(sizeof(*req) + groups * sizeof(gid_t));
	if (!req)
		return -ENOMEM;
	*req = (struct gsh_uring_request){
		.client = client,
		.op = op,
		.fd = fd,
		.write = write,
		.vector = vector,
		.stable = stable,
		.sync_batch_size = op->sync_batch_size,
		.sync_delay_us = op->sync_delay_us,
		.user = creds != NULL,
		.buf = buf,
		.len = len,
		.offset = offset,
	};
	if (creds) {
		req->creds = *creds;
		req->creds.caller_garray = req->groups;
		if (groups)
			memcpy(req->groups, creds->caller_garray,
			       groups * sizeof(gid_t));
	}
	pthread_mutex_lock(&st->lock);
	while (client->inflight >= client->max_inflight && !st->submit_error)
		pthread_cond_wait(&st->cv, &st->lock);
	ret = st->submit_error;
	if (!ret) {
		client->inflight++;
		wake = st->head == NULL && st->waiting;
		if (gsh_uring_can_group(req, req) && req->sync_delay_us) {
			clock_gettime(CLOCK_MONOTONIC, &req->collect_until);
			req->collect_until.tv_nsec += req->sync_delay_us * 1000;
			if (req->collect_until.tv_nsec >= 1000000000) {
				req->collect_until.tv_sec++;
				req->collect_until.tv_nsec -= 1000000000;
			}
		}
		/*
		 * Partial arrivals need no wake: the owner already has a
		 * deadline. A full batch or an incompatible request ends
		 * collection early.
		 */
		if (st->collecting &&
		    (!gsh_uring_can_group(st->head, req) ||
		     ++st->collect_count >= st->head->sync_batch_size))
			wake = true;
		if (st->tail)
			st->tail->next = req;
		else
			st->head = req;
		st->tail = req;
		if (wake) {
			st->collecting = false;
			gsh_uring_wake(st);
		}
	}
	pthread_mutex_unlock(&st->lock);
	if (ret)
		free(req);
	return ret;
}

int gsh_uring_submit_rw(int fd, bool is_write, void *buf, unsigned int len,
			uint64_t offset, bool stable, struct gsh_uring_op *op,
			const struct user_cred *creds)
{
	return gsh_uring_queue(fd, is_write, false, buf, len, offset, stable,
			       op, creds);
}

int gsh_uring_submit_rwv(int fd, bool is_write, const struct iovec *iov,
			 unsigned int nr_vecs, uint64_t offset, bool stable,
			 struct gsh_uring_op *op, const struct user_cred *creds)
{
	return gsh_uring_queue(fd, is_write, true, (void *)iov, nr_vecs, offset,
			       stable, op, creds);
}

#else /* !USE_GSH_IO_URING */

int gsh_uring_ensure(unsigned int queue_depth)
{
	(void)queue_depth;
	return -ENOSYS;
}

bool gsh_uring_in_reaper(void)
{
	return false;
}

int gsh_uring_submit_rw(int fd, bool is_write, void *buf, unsigned int len,
			uint64_t offset, bool stable, struct gsh_uring_op *op,
			const struct user_cred *creds)
{
	(void)fd;
	(void)is_write;
	(void)buf;
	(void)len;
	(void)offset;
	(void)stable;
	(void)op;
	(void)creds;
	return -ENOSYS;
}

int gsh_uring_submit_rwv(int fd, bool is_write, const struct iovec *iov,
			 unsigned int nr_vecs, uint64_t offset, bool stable,
			 struct gsh_uring_op *op, const struct user_cred *creds)
{
	(void)fd;
	(void)is_write;
	(void)iov;
	(void)nr_vecs;
	(void)offset;
	(void)stable;
	(void)op;
	(void)creds;
	return -ENOSYS;
}
#endif
