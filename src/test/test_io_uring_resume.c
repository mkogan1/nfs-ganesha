// SPDX-License-Identifier: LGPL-3.0-or-later
/* Exercise the real resume/release code with a replaceable callback wrapper. */
#ifndef TEST_GPFS
#include "config.h"
#include "gsh_io_uring.h"
static int capture_rw(int fd, bool write, void *buf, unsigned int len,
		      uint64_t offset, bool stable, struct gsh_uring_op *op);
static int capture_rwv(int fd, bool write, const struct iovec *iov,
		       unsigned int count, uint64_t offset, bool stable,
		       struct gsh_uring_op *op);
#define gsh_uring_submit_rw capture_rw
#define gsh_uring_submit_rwv capture_rwv
#endif
#ifdef TEST_GPFS
#include "../FSAL/FSAL_GPFS/gpfs_io_uring.c"
#define uring_cbi gpfs_uring_cbi
#define uring_resume gpfs_uring_resume
#else
#include "../FSAL/FSAL_VFS/vfs_io_uring.c"
#define uring_cbi vfs_uring_cbi
#define uring_resume vfs_uring_resume
#endif

#define CHECK(expr)                                                     \
	do {                                                            \
		if (!(expr)) {                                          \
			fprintf(stderr, "%s:%d: %s failed\n", __FILE__, \
				__LINE__, #expr);                       \
			abort();                                        \
		}                                                       \
	} while (0)

static unsigned int closes;

struct resume_callback_arg {
	struct fsal_obj_handle *obj;
	struct fsal_io_arg *arg;
	fsal_status_t status;
	unsigned int calls;
};

static fsal_status_t close_temp(struct fsal_obj_handle *obj, struct fsal_fd *fd)
{
	(void)obj;
	CHECK(fd->close_on_complete);
	closes++;
	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

static void stale_callback(struct fsal_obj_handle *obj, fsal_status_t status,
			   void *arg, void *caller_arg)
{
	(void)obj;
	(void)status;
	(void)arg;
	(void)caller_arg;
	CHECK(!"resumed I/O reused the original callback");
}

static void resumed_callback(struct fsal_obj_handle *obj, fsal_status_t status,
			     void *io_arg, void *caller_arg)
{
	struct resume_callback_arg *cb = caller_arg;
	struct fsal_io_arg *arg = io_arg;

	CHECK(cb->obj == obj);
	CHECK(cb->arg == arg);
	CHECK(cb->status.major == status.major);
	CHECK(cb->status.minor == status.minor);
	CHECK(arg->fsal_resume == FSAL_NORESUME);
	CHECK(arg->cbi == NULL);
	CHECK(closes == 1);
	cb->calls++;
}

static void test_resume(bool write, fsal_status_t status)
{
	struct fsal_obj_ops ops = { .close_func = close_temp };
	struct fsal_obj_handle obj = { .obj_ops = &ops };
	struct fsal_fd fd = { .close_on_complete = true };
	struct state_t state = { 0 };
	struct fsal_io_arg arg = { .state = &state,
				   .fsal_resume = FSAL_CLOSEFD };
	struct resume_callback_arg cb = { .obj = &obj,
					  .arg = &arg,
					  .status = status };
	struct uring_cbi *cbi = gsh_calloc(1, sizeof(*cbi), MEM_COMP_FSAL);

	CHECK(pthread_mutex_init(&cbi->lock, NULL) == 0);
	cbi->arg = &arg;
	cbi->obj_hdl = &obj;
	cbi->out_fd = &fd;
	cbi->is_write = write;
	cbi->status = status;
	cbi->done_cb = stale_callback;
	/* The original wrapper is already gone; only &cb is valid on resume. */
	cbi->caller_arg = NULL;
	arg.cbi = cbi;
	closes = 0;
	uring_resume(&obj, resumed_callback, &arg, &cb);
	CHECK(cb.calls == 1);
	CHECK(closes == 1);
}

#ifdef TEST_GPFS
static void test_vector_completion(bool write, bool stable, int result)
{
	struct fsal_obj_ops ops = { .close_func = close_temp };
	struct fsal_obj_handle obj = { .obj_ops = &ops };
	struct fsal_fd fd = { .close_on_complete = true };
	struct state_t state = { 0 };
	struct fsal_io_arg arg = { .state = &state };
	struct resume_callback_arg cb = {
		.obj = &obj,
		.arg = &arg,
		.status = result < 0 ? posix2fsal_status(-result)
				     : fsalstat(ERR_FSAL_NO_ERROR, 0),
	};
	struct gpfs_uring_cbi *cbi = gsh_calloc(1, sizeof(*cbi), MEM_COMP_FSAL);

	CHECK(pthread_mutex_init(&cbi->lock, NULL) == 0);
	cbi->arg = &arg;
	cbi->obj_hdl = &obj;
	cbi->out_fd = &fd;
	cbi->is_write = write;
	cbi->want_stable = stable;
	cbi->requested = 8;
	cbi->done_cb = resumed_callback;
	cbi->caller_arg = &cb;
	closes = 0;
	/* The kernel can finish before the submitting caller marks posted. */
	gpfs_uring_on_cqe(&cbi->op, result);
	CHECK(cb.calls == 0);
	CHECK(closes == 0);
	cbi->posted = true;
	CHECK(gpfs_uring_claim_locked(cbi));
	CHECK(!gpfs_uring_claim_locked(cbi));
	gpfs_uring_finish_inline(cbi);
	CHECK(cb.calls == 1);
	CHECK(arg.io_amount == (result < 0 ? 0 : (size_t)result));
	if (result >= 0) {
		if (write)
			CHECK(arg.fsal_stable == stable);
		else
			CHECK(arg.end_of_file == (result < 8));
	}
}

static void test_vector_limits(void)
{
	struct gpfs_fsal_export configured = { .use_io_uring = true };
	struct req_op_context ctx = { .fsal_export = &configured.export };
	struct req_op_context *saved_ctx = op_ctx;
	struct iovec iov = { 0 };
	struct fsal_io_arg arg = { .iov = &iov, .iov_count = IOV_MAX + 1 };
	struct gpfs_fsal_export *exp = NULL;

	op_ctx = &ctx;
	/* Reject excessive vectors before walking the caller's array. */
	CHECK(!gpfs_uring_wanted(&arg, false, &exp));
	CHECK(!gpfs_uring_wanted(&arg, true, &exp));
	arg.iov_count = IOV_MAX;
	CHECK(gpfs_uring_wanted(&arg, false, &exp));
	CHECK(gpfs_uring_wanted(&arg, true, &exp));
	CHECK(exp == &configured);
	op_ctx = saved_ctx;
}
#else
static struct vfs_uring_cbi *expected_submit;
static unsigned int scalar_calls, vector_calls;

static void check_submit(int fd, bool write, uint64_t offset, bool stable,
			 struct gsh_uring_op *op)
{
	CHECK(fd == expected_submit->fd);
	CHECK(write == expected_submit->is_write);
	CHECK(offset == expected_submit->arg->offset);
	CHECK(stable == expected_submit->want_stable);
	CHECK(op == &expected_submit->op);
}

static int capture_rw(int fd, bool write, void *buf, unsigned int len,
		      uint64_t offset, bool stable, struct gsh_uring_op *op)
{
	check_submit(fd, write, offset, stable, op);
	CHECK(buf == expected_submit->arg->iov[0].iov_base);
	CHECK(len == expected_submit->arg->iov[0].iov_len);
	scalar_calls++;
	return -EINTR;
}

static int capture_rwv(int fd, bool write, const struct iovec *iov,
		       unsigned int count, uint64_t offset, bool stable,
		       struct gsh_uring_op *op)
{
	check_submit(fd, write, offset, stable, op);
	CHECK(iov == expected_submit->arg->iov);
	CHECK(count == expected_submit->arg->iov_count);
	vector_calls++;
	return -EINTR;
}

static void test_submission_shape(bool write, bool stable)
{
	char buf[8];
	struct iovec iov[2] = { { buf, 4 }, { buf + 4, 4 } };
	struct fsal_io_arg arg = { .iov = iov, .iov_count = 1, .offset = 123 };
	struct vfs_uring_cbi cbi = {
		.arg = &arg,
		.fd = 42,
		.is_write = write,
		.want_stable = stable,
	};

	expected_submit = &cbi;
	scalar_calls = vector_calls = 0;
	CHECK(vfs_uring_submit(&cbi) == -EINTR);
	iov[0].iov_len = 0;
	CHECK(vfs_uring_submit(&cbi) == -EINTR);
	iov[0].iov_len = UINT32_MAX;
	CHECK(vfs_uring_submit(&cbi) == -EINTR);
	CHECK(scalar_calls == 3 && vector_calls == 0);
#if SIZE_MAX > UINT32_MAX
	iov[0].iov_len = (size_t)UINT32_MAX + 1;
	CHECK(vfs_uring_submit(&cbi) == -EINTR);
	CHECK(scalar_calls == 3 && vector_calls == 1);
#endif
	iov[0].iov_len = 4;
	arg.iov_count = 2;
	vector_calls = 0;
	CHECK(vfs_uring_submit(&cbi) == -EINTR);
	CHECK(scalar_calls == 3 && vector_calls == 1);
	expected_submit = NULL;
}
#endif

int main(void)
{
	component_log_level[COMPONENT_FSAL] = NIV_NULL;
	test_resume(false, fsalstat(ERR_FSAL_NO_ERROR, 0));
	test_resume(true, fsalstat(ERR_FSAL_NO_ERROR, 0));
	test_resume(false, fsalstat(ERR_FSAL_IO, EIO));
	test_resume(true, fsalstat(ERR_FSAL_IO, EIO));
#ifdef TEST_GPFS
	test_vector_completion(false, false, 8);
	test_vector_completion(false, false, 3);
	test_vector_completion(false, false, 0);
	test_vector_completion(false, false, -EIO);
	test_vector_completion(true, true, 8);
	test_vector_completion(true, true, 3);
	test_vector_completion(true, false, 8);
	test_vector_completion(true, true, -ENOSPC);
	test_vector_limits();
#else
	test_submission_shape(false, false);
	test_submission_shape(true, false);
	test_submission_shape(true, true);
#endif
	puts("io_uring resume callback tests passed");
	return 0;
}
