// SPDX-License-Identifier: LGPL-3.0-or-later
/* Inject failures into the real shared-ring code without kernel I/O. */
#include "config.h"
#include <errno.h>
#include <pthread.h>
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

static int results[32];
static unsigned int result_count, result_index;
static struct io_uring_cqe completion;
static int init_failure;

static int next_result(void)
{
	CHECK(result_index < result_count);
	return results[result_index++];
}

static int injected_submit(struct io_uring *ring)
{
	int ret = next_result();

	/* liburing publishes the SQ tail even when io_uring_enter fails. */
	ring->sq.sqe_head = ring->sq.sqe_tail;
	*ring->sq.ktail = ring->sq.sqe_tail;
	if (ret > 0) {
		CHECK((unsigned int)ret <= io_uring_sq_ready(ring));
		*ring->sq.khead += ret;
	}
	return ret;
}

static unsigned int injected_batch(struct io_uring *ring,
				   struct io_uring_cqe **cqes,
				   unsigned int count)
{
	(void)ring;
	(void)cqes;
	(void)count;
	/* Model the batch API hiding an unsuccessful overflow flush. */
	return 0;
}

static int injected_peek(struct io_uring *ring, struct io_uring_cqe **cqe)
{
	int ret = next_result();

	(void)ring;
	*cqe = ret == 0 ? &completion : NULL;
	return ret;
}

static int injected_key_create(pthread_key_t *key, void (*destroy)(void *))
{
	return init_failure == 1 ? EAGAIN : pthread_key_create(key, destroy);
}

static int injected_mutex_init(pthread_mutex_t *lock,
			       const pthread_mutexattr_t *attr)
{
	return init_failure == 2 ? ENOMEM : pthread_mutex_init(lock, attr);
}

static int injected_cond_init(pthread_cond_t *cv,
			      const pthread_condattr_t *attr)
{
	return init_failure == 3 ? ENOMEM : pthread_cond_init(cv, attr);
}

static int injected_setspecific(pthread_key_t key, const void *value)
{
	/* pthread functions return an error number; errno is unrelated. */
	errno = 0;
	return init_failure == 4 ? ENOMEM : pthread_setspecific(key, value);
}

#define io_uring_submit injected_submit
#define io_uring_peek_batch_cqe injected_batch
#define io_uring_peek_cqe injected_peek
#define pthread_key_create injected_key_create
#define pthread_mutex_init injected_mutex_init
#define pthread_cond_init injected_cond_init
#define pthread_setspecific injected_setspecific
#undef LogFatal
#define LogFatal(...) _exit(99)
#include "../support/gsh_io_uring.c"

static void script(int first, unsigned int count)
{
	unsigned int i;

	CHECK(count <= sizeof(results) / sizeof(results[0]));
	for (i = 0; i < count; i++)
		results[i] = first;
	result_count = count;
	result_index = 0;
}

static void unexpected_completion(struct gsh_uring_op *op, int res)
{
	(void)op;
	(void)res;
	CHECK(!"failed submission invoked its callback");
}

static void test_submission(void)
{
	struct gsh_uring_ring st = { .max_inflight = 8 };
	struct io_uring_sqe slots[8] = { 0 }, *sqe;
	struct gsh_uring_op op = { .complete = unexpected_completion };
	unsigned int head = 0, tail = 0, i;
	int errors[] = { -EINTR, -EAGAIN, -EBUSY, 0 };

	CHECK(pthread_mutex_init(&st.lock, NULL) == 0);
	CHECK(pthread_cond_init(&st.cv, NULL) == 0);
	st.ring.sq.khead = &head;
	st.ring.sq.ktail = &tail;
	st.ring.sq.sqes = slots;
	st.ring.sq.ring_entries = 8;
	st.ring.sq.ring_mask = 7;

	CHECK(gsh_uring_get_sqe(&st, &sqe) == 0);
	io_uring_prep_read(sqe, 10, NULL, 0, 0);
	script(-EINTR, 2);
	results[1] = 1;
	CHECK(gsh_uring_arm(&st, sqe, &op) == 0);
	CHECK(result_index == 2);
	CHECK(st.inflight == 1);
	CHECK(sqe->user_data == (uintptr_t)&op);
	/* Model this accepted request completing before the next request. */
	st.inflight--;

	CHECK(gsh_uring_get_sqe(&st, &sqe) == 0);
	io_uring_prep_write(sqe, 10, NULL, 0, 0);
	script(-EPERM, 1);
	CHECK(gsh_uring_arm(&st, sqe, &op) == -EPERM);
	CHECK(st.inflight == 0);
	CHECK(sqe->opcode == IORING_OP_NOP);
	CHECK(sqe->user_data == 0);
	for (i = 0; i < 20; i++) {
		/* Persistent failures must not accumulate more pending NOPs. */
		script(-EBADF, 1);
		CHECK(gsh_uring_get_sqe(&st, &sqe) == -EBADF);
		CHECK(st.inflight == 0);
		CHECK(io_uring_sq_ready(&st.ring) == 1);
	}
	script(1, 1);
	CHECK(gsh_uring_get_sqe(&st, &sqe) == 0);
	CHECK(st.inflight == 1);
	io_uring_prep_read(sqe, 10, NULL, 0, 0);
	script(1, 1);
	CHECK(gsh_uring_arm(&st, sqe, &op) == 0);
	st.inflight--;

	for (i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
		int expected = errors[i] == 0 ? -EAGAIN : errors[i];

		CHECK(gsh_uring_get_sqe(&st, &sqe) == 0);
		io_uring_prep_read(sqe, 10, NULL, 0, 0);
		script(errors[i], 16);
		CHECK(gsh_uring_arm(&st, sqe, &op) == expected);
		CHECK(result_index == 16);
		CHECK(st.inflight == 0);
		CHECK(sqe->opcode == IORING_OP_NOP);
		CHECK(sqe->user_data == 0);
		script(1, 1);
		CHECK(gsh_uring_submit_pending(&st) == 1);
	}
	/* Preserve an error even if earlier SQEs were consumed first. */
	CHECK(io_uring_get_sqe(&st.ring) != NULL);
	CHECK(io_uring_get_sqe(&st.ring) != NULL);
	script(1, 2);
	results[1] = -EIO;
	CHECK(gsh_uring_submit_pending(&st) == -EIO);
	CHECK(io_uring_sq_ready(&st.ring) == 1);
	CHECK(pthread_mutex_destroy(&st.lock) == 0);
	CHECK(pthread_cond_destroy(&st.cv) == 0);
}

static void test_completion(void)
{
	struct gsh_uring_ring st = { 0 };
	struct io_uring_cqe *cqes[GSH_URING_CQE_BATCH];

	script(-EINTR, 2);
	results[1] = 0;
	CHECK(gsh_uring_peek_batch(&st, cqes) == 1);
	CHECK(result_index == 2);
	CHECK(cqes[0] == &completion);
	script(-EAGAIN, 1);
	CHECK(gsh_uring_peek_batch(&st, cqes) == 0);
}

static void test_child(unsigned int scenario)
{
	pid_t pid = fork();
	int status;

	CHECK(pid >= 0);
	if (pid == 0) {
		if (scenario <= 4) {
			init_failure = scenario;
			errno = 0;
			CHECK(gsh_uring_ensure(128) ==
			      (scenario == 1 ? -EAGAIN : -ENOMEM));
			CHECK(gsh_uring_state() == NULL);
			_exit(0);
		} else {
			struct gsh_uring_ring st = { .event_fd = -1 };
			struct io_uring_cqe *cqes[GSH_URING_CQE_BATCH];

			if (scenario == 5) {
				script(-EBADF, 1);
				gsh_uring_peek_batch(&st, cqes);
			} else {
				gsh_uring_reaper(&st);
			}
			_exit(1);
		}
	}
	CHECK(waitpid(pid, &status, 0) == pid);
	CHECK(WIFEXITED(status));
	CHECK(WEXITSTATUS(status) == (scenario <= 4 ? 0 : 99));
}

int main(void)
{
	unsigned int i;

	alarm(10);
	component_log_level[COMPONENT_FSAL] = NIV_NULL;
	test_submission();
	test_completion();
	for (i = 1; i <= 6; i++)
		test_child(i);
	puts("io_uring submission, completion, and TLS failure tests passed");
	return 0;
}
