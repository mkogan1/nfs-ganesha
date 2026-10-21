// SPDX-License-Identifier: LGPL-3.0-or-later
/* Inject errors into the real batching code without requiring kernel rings. */
#include "config.h"
#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <liburing.h>
#undef UNUSED
#include "gsh_io_uring.h"
#include "log.h"

#define CHECK(expr)                                                     \
	do {                                                            \
		if (!(expr)) {                                          \
			fprintf(stderr, "%s:%d: %s failed\n", __FILE__, \
				__LINE__, #expr);                       \
			abort();                                        \
		}                                                       \
	} while (0)

static int results[32], init_failure;
static unsigned int result_count, result_index, setup_calls, setup_flags[2];

static void script(int result, unsigned int count)
{
	unsigned int i;

	CHECK(count <= 32);
	for (i = 0; i < count; i++)
		results[i] = result;
	result_count = count;
	result_index = 0;
}

static int injected_submit(struct io_uring *ring, unsigned int wait_nr)
{
	int ret;

	CHECK(wait_nr <= 1);
	CHECK(result_index < result_count);
	ret = results[result_index++];
	ring->sq.sqe_head = ring->sq.sqe_tail;
	*ring->sq.ktail = ring->sq.sqe_tail;
	if (ret > 0) {
		CHECK((unsigned int)ret <= io_uring_sq_ready(ring));
		*ring->sq.khead += ret;
	}
	return ret;
}

static int injected_setup(unsigned int entries, struct io_uring *ring,
			  struct io_uring_params *params)
{
	(void)entries;
	(void)ring;
	CHECK(setup_calls < 2);
	setup_flags[setup_calls++] = params->flags;
	CHECK(result_index < result_count);
	return results[result_index++];
}

static int injected_key(pthread_key_t *key, void (*destroy)(void *))
{
	return init_failure == 1 ? EAGAIN : pthread_key_create(key, destroy);
}
static int injected_mutex(pthread_mutex_t *lock, const pthread_mutexattr_t *a)
{
	return init_failure == 2 ? ENOMEM : pthread_mutex_init(lock, a);
}
static int injected_cond(pthread_cond_t *cv, const pthread_condattr_t *a)
{
	return init_failure == 3 ? ENOMEM : pthread_cond_init(cv, a);
}
static int injected_specific(pthread_key_t key, const void *value)
{
	errno = 0; /* pthread errors are return values, not errno. */
	return init_failure == 4 ? ENOMEM : pthread_setspecific(key, value);
}
static int injected_thread(pthread_t *t, const pthread_attr_t *a,
			   void *(*fn)(void *), void *arg)
{
	return init_failure == 5 ? EAGAIN : pthread_create(t, a, fn, arg);
}

static bool fake_clock;
static struct timespec fake_now;
static int wait_result;
static unsigned int timed_wait_calls, poll_wait_calls;
static struct timespec last_timeout;

static int injected_clock(clockid_t clock, struct timespec *now)
{
	if (!fake_clock)
		return clock_gettime(clock, now);
	CHECK(clock == CLOCK_MONOTONIC);
	*now = fake_now;
	return 0;
}

static int injected_timed_wait(struct io_uring *ring,
			       struct io_uring_cqe **cqe,
			       struct __kernel_timespec *ts)
{
	(void)ring;
	*cqe = NULL;
	timed_wait_calls++;
	last_timeout = (struct timespec){ ts->tv_sec, ts->tv_nsec };
	return wait_result;
}

static int injected_poll(struct pollfd *fds, nfds_t count,
			 const struct timespec *ts, const sigset_t *mask)
{
	CHECK(count == 2 && mask == NULL);
	CHECK(fds[0].events == POLLIN && fds[1].events == POLLIN);
	poll_wait_calls++;
	last_timeout = *ts;
	if (wait_result < 0) {
		errno = -wait_result;
		return -1;
	}
	return wait_result;
}

#define clock_gettime injected_clock
#define io_uring_wait_cqe_timeout injected_timed_wait
#define ppoll injected_poll
#ifdef IORING_SETUP_DEFER_TASKRUN
static int injected_workers(struct io_uring *ring, unsigned int *workers)
{
	(void)ring;
	CHECK(workers[0] == 8 && workers[1] == 0);
	return -EINVAL; /* Older kernels need not support the optional limit. */
}

#define io_uring_register_iowq_max_workers injected_workers
#endif
#define io_uring_submit_and_wait injected_submit
#define io_uring_queue_init_params injected_setup
#define pthread_key_create injected_key
#define pthread_mutex_init injected_mutex
#define pthread_cond_init injected_cond
#define pthread_setspecific injected_specific
#define pthread_create injected_thread
#include "../support/gsh_io_uring.c"

static unsigned int completions;
static struct gsh_uring_client *completing_client;
static void failed(struct gsh_uring_op *op, int res)
{
	(void)op;
	CHECK(res == -EIO);
	CHECK(completing_client->inflight == 3 - completions);
	completions++;
}

static void test_submission(void)
{
	struct gsh_uring_ring st = { 0 };
	struct io_uring_sqe slots[8] = { 0 }, *sqes[3];
	unsigned int head = 0, tail = 0, i;
	int errors[] = { -EINTR, -EAGAIN, -EBUSY, 0, -EPERM };
	struct gsh_uring_client client = { .ring = &st, .inflight = 3 };
	struct gsh_uring_op op = { .complete = failed };
	struct gsh_uring_request *batch[3];

	CHECK(pthread_mutex_init(&st.lock, NULL) == 0);
	CHECK(pthread_cond_init(&st.cv, NULL) == 0);
	st.ring.sq.khead = &head;
	st.ring.sq.ktail = &tail;
	st.ring.sq.sqes = slots;
	st.ring.sq.ring_entries = 8;
	st.ring.sq.ring_mask = 7;
	CHECK(io_uring_get_sqe(&st.ring));
	CHECK(io_uring_get_sqe(&st.ring));
	script(-EINTR, 3);
	results[1] = results[2] = 1;
	CHECK(gsh_uring_submit_pending(&st, 0) == 0);
	CHECK(result_index == 3);
	CHECK(io_uring_sq_ready(&st.ring) == 0);
	for (i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
		CHECK(io_uring_get_sqe(&st.ring));
		script(errors[i], 16);
		CHECK(gsh_uring_submit_pending(&st, 1) ==
		      (errors[i] ? errors[i] : -EAGAIN));
		CHECK(result_index == (errors[i] == -EPERM ? 1 : 16));
		CHECK(io_uring_sq_ready(&st.ring) == 1);
		script(1, 1);
		CHECK(gsh_uring_submit_pending(&st, 0) == 0);
	}
	/* A partial submission must never fail/free the accepted prefix. */
	for (i = 0; i < 3; i++) {
		batch[i] = calloc(1, sizeof(*batch[i]));
		CHECK(batch[i]);
		batch[i]->client = &client;
		batch[i]->op = &op;
		sqes[i] = io_uring_get_sqe(&st.ring);
		CHECK(sqes[i]);
		gsh_uring_prep(sqes[i], batch[i]);
	}
	st.active = 3;
	script(1, 2);
	results[1] = -EIO;
	CHECK(gsh_uring_submit_pending(&st, 0) == -EIO);
	completing_client = &client;
	gsh_uring_fail_batch(&st, batch, sqes, 3, -EIO);
	CHECK(completions == 2 && client.inflight == 1 && st.active == 1);
	CHECK(sqes[0]->user_data == (uintptr_t)batch[0]);
	for (i = 1; i < 3; i++) {
		CHECK(sqes[i]->opcode == IORING_OP_NOP);
		CHECK(sqes[i]->user_data == 0);
	}
	free(batch[0]);
	pthread_cond_destroy(&st.cv);
	pthread_mutex_destroy(&st.lock);
}

struct sync_test_op {
	struct gsh_uring_op op;
	unsigned int calls;
	int expected;
};

static void sync_test_complete(struct gsh_uring_op *op, int result)
{
	struct sync_test_op *test = (void *)op;

	CHECK(test->calls++ == 0);
	CHECK(result == test->expected);
}

/* Deterministic CQ ordering and sync-error propagation, including a short
 * write and an individual failed write. Credits survive all write CQEs.
 */
static void test_group_sync(int sync_result, bool partial_submit)
{
	struct gsh_uring_ring st = { 0 };
	struct gsh_uring_client client = { .ring = &st, .inflight = 8 };
	struct sync_test_op ops[8] = { 0 };
	struct gsh_uring_request *requests[8];
	struct io_uring_sqe sqe = { 0 };
	struct gsh_uring_sync *sync;
	unsigned int i;

	CHECK(pthread_mutex_init(&st.lock, NULL) == 0);
	CHECK(pthread_cond_init(&st.cv, NULL) == 0);
	for (i = 0; i < 8; i++) {
		requests[i] = calloc(1, sizeof(*requests[i]));
		CHECK(requests[i]);
		ops[i].op.complete = sync_test_complete;
		ops[i].expected = i == 2 ? -ENOSPC : (i == 5 ? 7 : 4096);
		if (partial_submit && i >= 4)
			ops[i].expected = -EIO;
		if (sync_result < 0 && ops[i].expected >= 0)
			ops[i].expected = sync_result;
		*requests[i] = (struct gsh_uring_request){
			.client = &client,
			.op = &ops[i].op,
			.fd = 123,
			.write = true,
			.stable = true,
			.sync_batch_size = 8,
			.len = 4096,
			.offset = i * 4096,
		};
	}
	gsh_uring_group_batch(&st, requests, 8);
	sync = requests[0]->sync;
	CHECK(sync && sync->pending == 8 && sync->count == 8);
	CHECK(st.sync_groups == 1 && st.grouped_writes == 8);
	for (i = 0; i < 8; i++) {
		gsh_uring_prep(&sqe, requests[i]);
		CHECK(sqe.opcode == IORING_OP_WRITE);
		CHECK(!(sqe.rw_flags & RWF_SYNC));
	}
	if (partial_submit) {
		struct io_uring_sqe slots[8] = { 0 }, *sqes[8];
		unsigned int head = 4, tail = 8;

		st.ring.sq.khead = &head;
		st.ring.sq.ktail = &tail;
		st.ring.sq.sqe_head = st.ring.sq.sqe_tail = 8;
		for (i = 0; i < 8; i++)
			sqes[i] = &slots[i];
		st.active = 8;
		gsh_uring_fail_batch(&st, requests, sqes, 8, -EIO);
		CHECK(st.active == 4 && sync->pending == 4);
		CHECK(client.inflight == 8 && st.sync_head == NULL);
	}
	for (i = partial_submit ? 4 : 8; i > 0; i--) {
		unsigned int index = i - 1;
		int res = index == 2 ? -ENOSPC : (index == 5 ? 7 : 4096);

		CHECK(st.sync_head == NULL);
		gsh_uring_sync_write_done(&st, requests[index], res);
		CHECK(client.inflight == 8 && ops[index].calls == 0);
	}
	CHECK(st.sync_head == sync && st.sync_tail == sync);
	CHECK(sync->pending == 0);
	requests[0]->sync_phase = true;
	gsh_uring_prep(&sqe, requests[0]);
	CHECK(sqe.opcode == IORING_OP_FSYNC);
	CHECK(sqe.fd == 123 && sqe.fsync_flags == 0);
	if (partial_submit) {
		/*
		 * Also exercise a rejected fsync SQE; no callback may succeed.
		 */
		struct io_uring_sqe *sqes[1] = { &sqe };
		unsigned int head = 0, tail = 1;

		st.ring.sq.khead = &head;
		st.ring.sq.ktail = &tail;
		st.ring.sq.sqe_head = st.ring.sq.sqe_tail = 1;
		st.active = 1;
		gsh_uring_fail_batch(&st, requests, sqes, 1, sync_result);
		CHECK(st.active == 0 && sqe.opcode == IORING_OP_NOP);
	} else {
		gsh_uring_sync_complete(&st, sync, sync_result);
	}
	CHECK(client.inflight == 0);
	CHECK(st.sync_errors == (sync_result < 0 ? 1 : 0));
	for (i = 0; i < 8; i++)
		CHECK(ops[i].calls == 1);
	pthread_cond_destroy(&st.cv);
	pthread_mutex_destroy(&st.lock);
}

static void test_group_boundaries(void)
{
	struct gsh_uring_ring st = { 0 };
	struct gsh_uring_request reqs[8] = { 0 }, *batch[8];
	struct io_uring_sqe sqe = { 0 };
	unsigned int i;

	for (i = 0; i < 8; i++) {
		reqs[i].write = reqs[i].stable = true;
		reqs[i].sync_batch_size = 2;
		reqs[i].fd = 50;
		batch[i] = &reqs[i];
	}
	/*
	 * Two groups of two, then fd, credential, policy, unstable boundaries.
	 */
	reqs[4].fd = 51;
	reqs[5].user = true;
	reqs[6].sync_delay_us = 25;
	reqs[7].stable = false;
	gsh_uring_group_batch(&st, batch, 8);
	CHECK(st.sync_groups == 2 && st.grouped_writes == 4);
	CHECK(st.single_stable == 3);
	CHECK(reqs[0].sync == reqs[1].sync);
	CHECK(reqs[2].sync == reqs[3].sync);
	CHECK(reqs[0].sync != reqs[2].sync);
	for (i = 4; i < 8; i++) {
		CHECK(reqs[i].sync == NULL);
		memset(&sqe, 0, sizeof(sqe));
		gsh_uring_prep(&sqe, &reqs[i]);
		CHECK(!!(sqe.rw_flags & RWF_SYNC) == (i != 7));
	}
	free(reqs[0].sync);
	free(reqs[2].sync);
}

/* A partial batch remains collectable with older I/O in flight. Reaping a
 * completion does not restart its deadline, and ready syncs take priority.
 */
static void test_collection(void)
{
	struct gsh_uring_ring st = { .entries = 128, .active = 3 };
	struct gsh_uring_request first = {
		.fd = 5, .write = true, .stable = true,
		.sync_batch_size = 4, .sync_delay_us = 1000,
		.collect_until = { 100, 1000000 },
	};
	struct gsh_uring_request incompatible = { .fd = 6 };
	struct gsh_uring_sync sync = { 0 };
	struct timespec until;
	struct gsh_uring_client client = { .ring = &st, .inflight = 1 };
	struct sync_test_op op = {
		.op.complete = sync_test_complete, .expected = 4096,
	};
	struct gsh_uring_request *done = calloc(1, sizeof(*done));
	struct io_uring_cqe cqe = { .res = 4096 };
	unsigned int head = 0, tail = 1;
	bool armed = true;

	CHECK(done);
	CHECK(pthread_mutex_init(&st.lock, NULL) == 0);
	CHECK(pthread_cond_init(&st.cv, NULL) == 0);
	fake_clock = true;
	fake_now = (struct timespec){ 100, 0 };
	st.head = &first;
	CHECK(gsh_uring_collect(&st, &until));
	CHECK(st.collect_count == 1 && until.tv_nsec == 1000000);
	done->client = &client;
	done->op = &op.op;
	cqe.user_data = (uintptr_t)done;
	st.ring.cq.khead = &head;
	st.ring.cq.ktail = &tail;
	st.ring.cq.ring_mask = 0;
	st.ring.cq.cqes = &cqe;
	CHECK(gsh_uring_reap(&st, &armed) == 1);
	CHECK(op.calls == 1 && client.inflight == 0 && st.active == 2);
	fake_now.tv_nsec = 250000;
	CHECK(gsh_uring_collect(&st, &until));
	CHECK(until.tv_sec == 100 && until.tv_nsec == 1000000);
	st.sync_head = &sync;
	CHECK(!gsh_uring_collect(&st, &until));
	st.sync_head = NULL;
	CHECK(gsh_uring_collect(&st, &until));
	first.next = &incompatible;
	CHECK(!gsh_uring_collect(&st, &until));
	first.next = NULL;
	st.active = 127;
	CHECK(!gsh_uring_collect(&st, &until));
	st.active = 2;

#ifdef IORING_FEAT_EXT_ARG
	st.ring.features = IORING_FEAT_EXT_ARG;
	wait_result = 0; /* A CQE wakes collection without being consumed. */
	CHECK(gsh_uring_wait_collect(&st, &until) == 0);
	CHECK(timed_wait_calls == 1 && head == 1);
	CHECK(last_timeout.tv_sec == 0 && last_timeout.tv_nsec == 750000);
	wait_result = -ETIME;
	CHECK(gsh_uring_wait_collect(&st, &until) == 0);
	wait_result = -EINTR;
	CHECK(gsh_uring_wait_collect(&st, &until) == 0);
	wait_result = -EIO;
	CHECK(gsh_uring_wait_collect(&st, &until) == -EIO);
#endif
	st.ring.features = 0;
	wait_result = 1;
	CHECK(gsh_uring_wait_collect(&st, &until) == 0);
	CHECK(poll_wait_calls == 1 && last_timeout.tv_nsec == 750000);
	wait_result = -EINTR;
	CHECK(gsh_uring_wait_collect(&st, &until) == 0);
	wait_result = -EBADF;
	CHECK(gsh_uring_wait_collect(&st, &until) == -EBADF);
	fake_now.tv_nsec = 1000000;
	CHECK(!gsh_uring_collect(&st, &until));
	CHECK(gsh_uring_wait_collect(&st, &until) == 0);
	CHECK(poll_wait_calls == 3); /* Expired deadline never sleeps again. */
	fake_clock = false;
	pthread_cond_destroy(&st.cv);
	pthread_mutex_destroy(&st.lock);
}

/* Producer wakes are needed at the full/incompatible boundary, even though
 * the queue was already nonempty. Partial arrivals retain the first deadline.
 */
static void test_collection_wakes(void)
{
	struct gsh_uring_ring st = { .entries = 128, .active = 3 };
	struct gsh_uring_client client = { .ring = &st, .max_inflight = 128 };
	struct gsh_uring_op op = {
		.complete = failed, .sync_batch_size = 4, .sync_delay_us = 1000,
	};
	struct timespec until;
	struct gsh_uring_request *req;
	uint64_t value;
	unsigned int i;

	CHECK(pthread_mutex_init(&st.lock, NULL) == 0);
	CHECK(pthread_cond_init(&st.cv, NULL) == 0);
	st.event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	CHECK(st.event_fd >= 0);
	(void)gsh_uring_state();
	CHECK(pthread_setspecific(gsh_uring_key, &client) == 0);
	fake_clock = true;
	fake_now = (struct timespec){ 200, 999800000 };
	CHECK(gsh_uring_submit_rw(5, true, NULL, 0, 0, true, &op, NULL) == 0);
	CHECK(gsh_uring_collect(&st, &until));
	CHECK(until.tv_sec == 201 && until.tv_nsec == 800000);
	st.collecting = st.waiting = true;
	fake_now = (struct timespec){ 201, 0 };
	for (i = 0; i < 2; i++) {
		CHECK(gsh_uring_submit_rw(5, true, NULL, 0, 0, true, &op,
					  NULL) == 0);
		CHECK(read(st.event_fd, &value, sizeof(value)) == -1 &&
		      errno == EAGAIN);
	}
	CHECK(gsh_uring_submit_rw(5, true, NULL, 0, 0, true, &op, NULL) == 0);
	CHECK(read(st.event_fd, &value, sizeof(value)) == sizeof(value) &&
	      value == 1);
	CHECK(!st.collecting && !gsh_uring_collect(&st, &until));
	CHECK(st.head->collect_until.tv_sec == 201 &&
	      st.head->collect_until.tv_nsec == 800000);
	/*
	 * Remove all but the first queued request, then add an incompatible fd.
	 */
	while ((req = st.head->next) != NULL) {
		st.head->next = req->next;
		free(req);
		client.inflight--;
	}
	st.tail = st.head;
	CHECK(gsh_uring_collect(&st, &until));
	st.collecting = true;
	CHECK(gsh_uring_submit_rw(6, true, NULL, 0, 0, true, &op, NULL) == 0);
	CHECK(read(st.event_fd, &value, sizeof(value)) == sizeof(value) &&
	      value == 1);
	CHECK(!st.collecting && !gsh_uring_collect(&st, &until));
	while ((req = st.head) != NULL) {
		st.head = req->next;
		free(req);
	}
	CHECK(pthread_setspecific(gsh_uring_key, NULL) == 0);
	fake_clock = false;
	close(st.event_fd);
	pthread_cond_destroy(&st.cv);
	pthread_mutex_destroy(&st.lock);
}

static void test_setup(void)
{
	struct gsh_uring_ring st = { .entries = 128 };

#if defined(IORING_SETUP_SINGLE_ISSUER) &&     \
	defined(IORING_SETUP_DEFER_TASKRUN) && \
	defined(IORING_SETUP_COOP_TASKRUN)
	setup_calls = 0;
	script(-EINVAL, 2);
	results[1] = 0;
	CHECK(gsh_uring_setup(&st) == 0);
	CHECK(setup_calls == 2 && setup_flags[1] == 0);
	CHECK(setup_flags[0] & IORING_SETUP_SINGLE_ISSUER);
	CHECK(setup_flags[0] & IORING_SETUP_DEFER_TASKRUN);
#endif
	setup_calls = 0;
	script(-ENOSYS, 1);
	CHECK(gsh_uring_setup(&st) == -ENOSYS);
	CHECK(setup_calls == 1);
}

static void test_credentials(void)
{
	struct gsh_uring_ring st = { 0 };
	struct gsh_uring_client client = { .ring = &st, .max_inflight = 128 };
	gid_t groups[] = { 51, 52 };
	struct user_cred creds = { .caller_uid = 42,
				   .caller_gid = 43,
				   .caller_glen = 2,
				   .caller_garray = groups };
	struct gsh_uring_op op = { .complete = failed };
	struct gsh_uring_request *req;

	CHECK(pthread_mutex_init(&st.lock, NULL) == 0);
	CHECK(pthread_cond_init(&st.cv, NULL) == 0);
	st.event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	CHECK(st.event_fd >= 0);
	(void)gsh_uring_state();
	CHECK(pthread_setspecific(gsh_uring_key, &client) == 0);
	CHECK(gsh_uring_submit_rw(3, true, NULL, 0, 0, true, &op, &creds) == 0);
	req = st.head;
	CHECK(req && client.inflight == 1);
	creds.caller_uid = 100;
	groups[0] = 99;
	CHECK(req->creds.caller_uid == 42 && req->groups[0] == 51);
	CHECK(req->creds.caller_garray == req->groups);
	CHECK(gsh_uring_same_user(req, req));
	st.head = st.tail = NULL;
	free(req);
	client.inflight = 0;
	st.submit_error = -EIO;
	CHECK(gsh_uring_submit_rw(3, true, NULL, 0, 0, true, &op, &creds) ==
	      -EIO);
	CHECK(client.inflight == 0 && st.head == NULL);
	CHECK(pthread_setspecific(gsh_uring_key, NULL) == 0);
	close(st.event_fd);
	pthread_cond_destroy(&st.cv);
	pthread_mutex_destroy(&st.lock);
}

static void test_init(unsigned int scenario)
{
	pid_t child = fork();
	int status;

	CHECK(child >= 0);
	if (child == 0) {
		if (scenario == 6) {
			script(-ENOSYS, 1);
			CHECK(gsh_uring_ensure(128) == -ENOSYS);
			CHECK(gsh_uring_ensure(128) == -ENOSYS);
			CHECK(setup_calls == 1);
			gsh_uring_client_destroy(gsh_uring_state());
		} else {
			init_failure = scenario;
			CHECK(gsh_uring_ensure(128) ==
			      ((scenario == 1 || scenario == 5) ? -EAGAIN
								: -ENOMEM));
			CHECK(gsh_uring_state() == NULL);
		}
		_exit(0);
	}
	CHECK(waitpid(child, &status, 0) == child);
	CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

int main(void)
{
	unsigned int i;
	alarm(10);
	component_log_level[COMPONENT_FSAL] = NIV_NULL;
	/* Run before the parent initializes its pthread_once/key. */
	for (i = 1; i <= 6; i++)
		test_init(i);
	test_submission();
	test_group_sync(0, false);
	test_group_sync(-EIO, false);
	test_group_sync(-EIO, true);
	test_group_boundaries();
	test_collection();
	test_collection_wakes();
	test_setup();
	test_credentials();
	puts("io_uring batch, credential-copy, setup fallback, "
	     "and failure tests passed");
	return 0;
}
