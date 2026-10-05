// SPDX-License-Identifier: LGPL-3.0-or-later
/* Run with --disabled to exercise a WITH_SYSTEM_LIBURING=OFF build. */
#include "config.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "gsh_io_uring.h"
#include "log.h"

/* Unlike assert(), these checks remain enabled in release builds. */
#define CHECK(expr)                                                     \
	do {                                                            \
		if (!(expr)) {                                          \
			fprintf(stderr, "%s:%d: %s failed\n", __FILE__, \
				__LINE__, #expr);                       \
			abort();                                        \
		}                                                       \
	} while (0)

struct test_io {
	struct gsh_uring_op op;
	pthread_mutex_t lock;
	pthread_cond_t cv;
	unsigned int completed;
	int result;
	int fd;
	char byte;
	bool submitted;
};

static void complete(struct gsh_uring_op *op, int result)
{
	struct test_io *io = (struct test_io *)op;

	CHECK(gsh_uring_in_reaper());
	pthread_mutex_lock(&io->lock);
	io->completed++;
	io->result = result;
	pthread_cond_signal(&io->cv);
	pthread_mutex_unlock(&io->lock);
}

static void init_io(struct test_io *io)
{
	memset(io, 0, sizeof(*io));
	io->op.complete = complete;
	CHECK(pthread_mutex_init(&io->lock, NULL) == 0);
	CHECK(pthread_cond_init(&io->cv, NULL) == 0);
}

static void destroy_io(struct test_io *io)
{
	CHECK(pthread_mutex_destroy(&io->lock) == 0);
	CHECK(pthread_cond_destroy(&io->cv) == 0);
}

static void wait_io(struct test_io *io, unsigned int count, int result)
{
	pthread_mutex_lock(&io->lock);
	while (io->completed < count)
		pthread_cond_wait(&io->cv, &io->lock);
	CHECK(io->completed == count);
	CHECK(io->result == result);
	pthread_mutex_unlock(&io->lock);
}

static void *test_file_io(void *unused)
{
	struct test_io io;
	char name[] = "/tmp/ganesha-uring-test.XXXXXX";
	char data[] = "abcd", readback[sizeof(data)] = { 0 };
	struct iovec iov[] = {
		{ .iov_base = data, .iov_len = 2 },
		{ .iov_base = data + 2, .iov_len = 2 },
	};
	uint64_t bad_offsets[] = { UINT64_MAX, (uint64_t)INT64_MAX + 1 };
	int fd;
	unsigned int i;

	(void)unused;
	init_io(&io);
	CHECK(gsh_uring_ensure(128) == 0);
	fd = mkstemp(name);
	CHECK(fd >= 0);
	CHECK(unlink(name) == 0);
	CHECK(write(fd, data, 4) == 4);
	CHECK(lseek(fd, 0, SEEK_SET) == 0);

	/* Neither invalid submission may arm a callback or alter the file. */
	for (i = 0; i < sizeof(bad_offsets) / sizeof(bad_offsets[0]); i++) {
		CHECK(gsh_uring_submit_rw(fd, true, data, 4, bad_offsets[i],
					  false, &io.op) == -EINVAL);
		CHECK(gsh_uring_submit_rwv(fd, true, iov, 2, bad_offsets[i],
					   false, &io.op) == -EINVAL);
		CHECK(gsh_uring_submit_rw(fd, false, readback, 4,
					  bad_offsets[i], false,
					  &io.op) == -EINVAL);
		CHECK(gsh_uring_submit_rwv(fd, false, iov, 2, bad_offsets[i],
					   false, &io.op) == -EINVAL);
	}
	CHECK(gsh_uring_submit_rw(fd, true, data, 4, 0, false, NULL) ==
	      -EINVAL);
	CHECK(gsh_uring_submit_rwv(fd, true, iov, 2, 0, false, NULL) ==
	      -EINVAL);

	/* Exercise both stable-write SQE types and both read types. */
	CHECK(gsh_uring_submit_rw(fd, true, data, 4, 4, true, &io.op) == 0);
	wait_io(&io, 1, 4);
	CHECK(gsh_uring_submit_rwv(fd, true, iov, 2, 8, true, &io.op) == 0);
	wait_io(&io, 2, 4);
	CHECK(gsh_uring_submit_rw(fd, false, readback, 4, 4, false, &io.op) ==
	      0);
	wait_io(&io, 3, 4);
	CHECK(memcmp(data, readback, 4) == 0);
	memset(readback, 0, sizeof(readback));
	iov[0].iov_base = readback;
	iov[1].iov_base = readback + 2;
	CHECK(gsh_uring_submit_rwv(fd, false, iov, 2, 8, false, &io.op) == 0);
	wait_io(&io, 4, 4);
	CHECK(memcmp(data, readback, 4) == 0);
	/* A short vectored read fills only a prefix of the supplied buffers. */
	memset(readback, '?', sizeof(readback));
	CHECK(gsh_uring_submit_rwv(fd, false, iov, 2, 10, false, &io.op) == 0);
	wait_io(&io, 5, 2);
	CHECK(memcmp(readback, "cd??", 4) == 0);
	CHECK(gsh_uring_submit_rwv(fd, false, iov, 2, 12, false, &io.op) == 0);
	wait_io(&io, 6, 0);
	CHECK(memcmp(readback, "cd??", 4) == 0);
	CHECK(gsh_uring_submit_rwv(-1, false, iov, 2, 0, false, &io.op) == 0);
	wait_io(&io, 7, -EBADF);
	CHECK(lseek(fd, 0, SEEK_CUR) == 0);
	CHECK(pread(fd, readback, 4, 0) == 4);
	CHECK(memcmp(data, readback, 4) == 0);
	CHECK(close(fd) == 0);
	destroy_io(&io);
	return NULL;
}

static void *submit_then_exit(void *arg)
{
	struct test_io *io = arg;

	CHECK(gsh_uring_ensure(128) == 0);
	CHECK(gsh_uring_submit_rw(io->fd, false, &io->byte, 1, 0, false,
				  &io->op) == 0);
	pthread_mutex_lock(&io->lock);
	io->submitted = true;
	pthread_cond_signal(&io->cv);
	pthread_mutex_unlock(&io->lock);
	/* The TLS destructor must drain the still-pending pipe read. */
	return NULL;
}

static void test_pending_exit(void)
{
	struct test_io io;
	struct timespec until;
	pthread_t thread;
	int fds[2];

	init_io(&io);
	CHECK(pipe(fds) == 0);
	io.fd = fds[0];
	CHECK(pthread_create(&thread, NULL, submit_then_exit, &io) == 0);
	pthread_mutex_lock(&io.lock);
	while (!io.submitted)
		pthread_cond_wait(&io.cv, &io.lock);
	pthread_mutex_unlock(&io.lock);
	CHECK(clock_gettime(CLOCK_REALTIME, &until) == 0);
	until.tv_sec++;
	CHECK(pthread_timedjoin_np(thread, NULL, &until) == ETIMEDOUT);
	CHECK(write(fds[1], "x", 1) == 1);
	CHECK(pthread_join(thread, NULL) == 0);
	/* Joining includes teardown, so no callback may remain outstanding. */
	CHECK(io.completed == 1);
	CHECK(io.result == 1);
	CHECK(io.byte == 'x');
	CHECK(close(fds[0]) == 0);
	CHECK(close(fds[1]) == 0);
	destroy_io(&io);
}

#define TEST_DEPTH 128
#define TEST_REQUESTS (3 * TEST_DEPTH + 5)

struct depth_test {
	pthread_mutex_t lock;
	pthread_cond_t cv;
	int fd;
	bool filled;
	bool submitted_more;
	bool callback_entered;
	bool allow_callbacks;
	unsigned int completed;
	unsigned int seen[TEST_REQUESTS];
};

struct depth_op {
	struct gsh_uring_op op;
	struct depth_test *test;
	unsigned int index;
	char byte;
	struct iovec iov;
};

static void depth_complete(struct gsh_uring_op *op, int result)
{
	struct depth_op *req = (struct depth_op *)op;
	struct depth_test *test = req->test;

	CHECK(gsh_uring_in_reaper());
	CHECK(result == 1);
	CHECK(req->byte == 'x');
	pthread_mutex_lock(&test->lock);
	if (!test->callback_entered) {
		test->callback_entered = true;
		pthread_cond_signal(&test->cv);
		while (!test->allow_callbacks)
			pthread_cond_wait(&test->cv, &test->lock);
	}
	CHECK(test->seen[req->index]++ == 0);
	test->completed++;
	pthread_mutex_unlock(&test->lock);
	/* Reaping a batch must never access an op after its callback. */
	free(req);
}

static void *fill_ring(void *arg)
{
	struct depth_test *test = arg;
	unsigned int i;

	CHECK(gsh_uring_ensure(TEST_DEPTH) == 0);
	/* A later export cannot enlarge this worker's existing limit. */
	CHECK(gsh_uring_ensure(4096) == 0);
	for (i = 0; i < TEST_REQUESTS; i++) {
		struct depth_op *req = calloc(1, sizeof(*req));

		CHECK(req != NULL);
		req->op.complete = depth_complete;
		req->test = test;
		req->index = i;
		req->iov.iov_base = &req->byte;
		req->iov.iov_len = 1;
		if (i % 2 == 0)
			CHECK(gsh_uring_submit_rw(test->fd, false, &req->byte,
						  1, 0, false, &req->op) == 0);
		else
			CHECK(gsh_uring_submit_rwv(test->fd, false, &req->iov,
						   1, 0, false, &req->op) == 0);
		pthread_mutex_lock(&test->lock);
		if (i == TEST_DEPTH - 1)
			test->filled = true;
		if (i == TEST_DEPTH)
			test->submitted_more = true;
		pthread_cond_signal(&test->cv);
		pthread_mutex_unlock(&test->lock);
	}
	return NULL;
}

/* Caller holds test->lock. No completion credit should be available yet. */
static void check_submission_blocked(struct depth_test *test)
{
	struct timespec until;
	int rc = 0;

	CHECK(clock_gettime(CLOCK_REALTIME, &until) == 0);
	until.tv_sec++;
	while (!test->submitted_more && rc == 0)
		rc = pthread_cond_timedwait(&test->cv, &test->lock, &until);
	CHECK(rc == ETIMEDOUT);
	CHECK(!test->submitted_more);
}

static void test_depth_and_batches(void)
{
	struct depth_test test = { 0 };
	pthread_t thread;
	char bytes[TEST_REQUESTS];
	int fds[2];
	unsigned int i;

	CHECK(pthread_mutex_init(&test.lock, NULL) == 0);
	CHECK(pthread_cond_init(&test.cv, NULL) == 0);
	CHECK(pipe(fds) == 0);
	test.fd = fds[0];
	CHECK(pthread_create(&thread, NULL, fill_ring, &test) == 0);
	pthread_mutex_lock(&test.lock);
	while (!test.filled)
		pthread_cond_wait(&test.cv, &test.lock);
	/* SQ slots are free now, but all 128 reads are still outstanding. */
	check_submission_blocked(&test);
	pthread_mutex_unlock(&test.lock);
	memset(bytes, 'x', sizeof(bytes));
	CHECK(write(fds[1], bytes, sizeof(bytes)) == sizeof(bytes));
	pthread_mutex_lock(&test.lock);
	while (!test.callback_entered)
		pthread_cond_wait(&test.cv, &test.lock);
	/* CQEs have arrived, but a blocked callback must retain its credit. */
	check_submission_blocked(&test);
	test.allow_callbacks = true;
	pthread_cond_signal(&test.cv);
	pthread_mutex_unlock(&test.lock);
	CHECK(pthread_join(thread, NULL) == 0);
	CHECK(test.submitted_more);
	CHECK(test.completed == TEST_REQUESTS);
	for (i = 0; i < TEST_REQUESTS; i++)
		CHECK(test.seen[i] == 1);
	CHECK(close(fds[0]) == 0);
	CHECK(close(fds[1]) == 0);
	CHECK(pthread_mutex_destroy(&test.lock) == 0);
	CHECK(pthread_cond_destroy(&test.cv) == 0);
}

int main(int argc, char **argv)
{
	pthread_t thread;
	struct test_io io;
	struct iovec iov = { .iov_base = &io.byte, .iov_len = 1 };

	/* Bound regressions that strand a callback or deadlock teardown. */
	alarm(30);
	/* No daemon logging facilities are initialized in these tests. */
	component_log_level[COMPONENT_FSAL] = NIV_NULL;
	if (argc == 2 && strcmp(argv[1], "--disabled") == 0) {
		init_io(&io);
		CHECK(gsh_uring_ensure(128) == -ENOSYS);
		CHECK(!gsh_uring_in_reaper());
		CHECK(gsh_uring_submit_rw(-1, false, &io.byte, 1, 0, false,
					  &io.op) == -ENOSYS);
		CHECK(gsh_uring_submit_rwv(-1, false, &iov, 1, 0, false,
					   &io.op) == -ENOSYS);
		CHECK(io.completed == 0);
		destroy_io(&io);
		puts("io_uring disabled-build tests passed");
		return 0;
	}
	CHECK(argc == 1);
	CHECK(pthread_create(&thread, NULL, test_file_io, NULL) == 0);
	CHECK(pthread_join(thread, NULL) == 0);
	test_pending_exit();
	test_depth_and_batches();
	puts("io_uring offset, file I/O, pending-exit, and depth tests passed");
	return 0;
}
