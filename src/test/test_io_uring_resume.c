// SPDX-License-Identifier: LGPL-3.0-or-later
/* Exercise the real resume/release code with a replaceable callback wrapper. */
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

int main(void)
{
	component_log_level[COMPONENT_FSAL] = NIV_NULL;
	test_resume(false, fsalstat(ERR_FSAL_NO_ERROR, 0));
	test_resume(true, fsalstat(ERR_FSAL_NO_ERROR, 0));
	test_resume(false, fsalstat(ERR_FSAL_IO, EIO));
	test_resume(true, fsalstat(ERR_FSAL_IO, EIO));
	puts("io_uring resume callback tests passed");
	return 0;
}
