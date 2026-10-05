/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file gpfs_io_uring.c
 * @brief FSAL_GPFS caller for the shared io_uring ring.
 *
 * The ring lives in gsh_io_uring. This file owns the GPFS file descriptor,
 * the OPENHANDLE ioctl fallback, and READ_PLUS / IO_SKIP_HOLE, which stays
 * on the ioctl path. Stable writes are RWF_SYNC on the SQE.
 */
#include "config.h"
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include "fsal.h"
#include "fsal_convert.h"
#include "FSAL/access_check.h"
#include "FSAL/fsal_commonlib.h"
#include "abstract_mem.h"
#include "export_mgr.h"
#include "fsal_internal.h"
#include "gsh_io_uring.h"
#include "gpfs_methods.h"
#include "gpfs_io_uring.h"

struct gpfs_uring_sqe {
	struct gsh_uring_op op;
	struct gpfs_uring_cbi *cbi;
	int index;
	size_t len;
	int res;
	bool armed;
	bool have_res;
};

struct gpfs_uring_cbi {
	struct fsal_io_arg *arg;
	struct gsh_export *exp;
	struct fsal_export *fsal_export;
	struct fsal_obj_handle *obj_hdl;
	fsal_async_cb done_cb;
	void *caller_arg;
	struct gpfs_fd temp_fd;
	struct fsal_fd *out_fd;
	int fd;
	int export_fd;
	bool is_write;
	bool want_stable;
	bool posted;
	bool finished;
	int force_error;
	int iov_count;
	int armed;
	int got;
	fsal_status_t status;
	pthread_mutex_t lock;
	struct gpfs_uring_sqe sqe[];
};

static bool gpfs_uring_wanted(struct fsal_io_arg *arg, bool is_write,
			      struct gpfs_fsal_export **exp_out)
{
	struct gpfs_fsal_export *exp;

	if (gsh_uring_in_reaper())
		return false;
	/* READ_PLUS / IO_SKIP_HOLE stays on the ioctl path. */
	if (!is_write && arg->info != NULL)
		return false;
	if (arg->iov == NULL || arg->iov_count <= 0)
		return false;
	if (op_ctx == NULL || op_ctx->fsal_export == NULL)
		return false;
	exp = container_of(op_ctx->fsal_export, struct gpfs_fsal_export, export);
	if (!exp->use_io_uring)
		return false;
	*exp_out = exp;
	return true;
}

static void gpfs_uring_apply_results(struct gpfs_uring_cbi *cbi)
{
	struct fsal_io_arg *arg = cbi->arg;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	size_t amount = 0;
	bool eof = false;
	int i;

	for (i = 0; i < cbi->iov_count; i++) {
		struct gpfs_uring_sqe *sqe = &cbi->sqe[i];

		if (!sqe->armed || !sqe->have_res) {
			status = posix2fsal_status(EIO);
			break;
		}
		if (sqe->res < 0) {
			status = posix2fsal_status(-sqe->res);
			break;
		}
		amount += (size_t)sqe->res;
		if (!cbi->is_write)
			eof = (sqe->res == 0 || (size_t)sqe->res < sqe->len);
	}
	if (cbi->force_error && !FSAL_IS_ERROR(status))
		status = posix2fsal_status(cbi->force_error);
	arg->io_amount = amount;
	if (!cbi->is_write)
		arg->end_of_file = eof;
	else if (!FSAL_IS_ERROR(status))
		arg->fsal_stable = cbi->want_stable;
	cbi->status = status;
}

static void gpfs_uring_release(struct gpfs_uring_cbi *cbi)
{
	fsal_status_t status2;
	struct fsal_obj_handle *obj = cbi->obj_hdl;
	struct gpfs_fsal_obj_handle *myself;
	fsal_openflags_t was;

	status2 = fsal_complete_io(obj, cbi->out_fd);
	LogFullDebug(COMPONENT_FSAL, "fsal_complete_io returned %s",
		     fsal_err_txt(status2));
	if (cbi->arg->state == NULL) {
		myself = container_of(obj, struct gpfs_fsal_obj_handle,
				      obj_handle);
		was = cbi->is_write ? FSAL_O_WRITE : FSAL_O_READ;
		update_share_counters_locked(obj, &myself->u.file.share, was,
					     FSAL_O_CLOSED);
	}
	cbi->done_cb(obj, cbi->status, cbi->arg, cbi->caller_arg);
	pthread_mutex_destroy(&cbi->lock);
	gsh_free(cbi, MEM_COMP_FSAL);
}

static void gpfs_uring_finish_inline(struct gpfs_uring_cbi *cbi)
{
	gpfs_uring_apply_results(cbi);
	gpfs_uring_release(cbi);
}

static void gpfs_uring_finish_on_reaper(struct gpfs_uring_cbi *cbi)
{
	struct req_op_context ctx;

	gpfs_uring_apply_results(cbi);
	if (cbi->exp == NULL || cbi->fsal_export == NULL) {
		LogMajor(COMPONENT_FSAL,
			 "GPFS io_uring: completion without export");
		cbi->done_cb(cbi->obj_hdl, cbi->status, cbi->arg,
			     cbi->caller_arg);
		pthread_mutex_destroy(&cbi->lock);
		gsh_free(cbi, MEM_COMP_FSAL);
		return;
	}
	get_gsh_export_ref(cbi->exp);
	init_op_context_simple(&ctx, cbi->exp, cbi->fsal_export);
	if (cbi->out_fd->close_on_complete) {
		cbi->arg->fsal_resume = FSAL_CLOSEFD;
		cbi->arg->cbi = cbi;
		cbi->done_cb(cbi->obj_hdl, cbi->status, cbi->arg,
			     cbi->caller_arg);
		release_op_context();
		return;
	}
	gpfs_uring_release(cbi);
	release_op_context();
}

static void gpfs_uring_finish(struct gpfs_uring_cbi *cbi)
{
	if (gsh_uring_in_reaper())
		gpfs_uring_finish_on_reaper(cbi);
	else
		gpfs_uring_finish_inline(cbi);
}

static bool gpfs_uring_claim_locked(struct gpfs_uring_cbi *cbi)
{
	bool run;

	run = cbi->posted && !cbi->finished && cbi->got == cbi->armed;
	if (run)
		cbi->finished = true;
	return run;
}

static void gpfs_uring_on_cqe(struct gsh_uring_op *op, int res)
{
	struct gpfs_uring_sqe *self =
		container_of(op, struct gpfs_uring_sqe, op);
	struct gpfs_uring_cbi *cbi = self->cbi;
	bool run;

	pthread_mutex_lock(&cbi->lock);
	self->res = res;
	self->have_res = true;
	cbi->got++;
	run = gpfs_uring_claim_locked(cbi);
	pthread_mutex_unlock(&cbi->lock);
	if (run)
		gpfs_uring_finish(cbi);
}

static void gpfs_uring_ioctl_fallback(struct gpfs_uring_cbi *cbi)
{
	struct fsal_io_arg *arg = cbi->arg;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };
	uint64_t offset = arg->offset;
	int i;

	arg->io_amount = 0;
	for (i = 0; i < arg->iov_count; i++) {
		size_t io_amount = 0;

		if (cbi->is_write)
			status = GPFSFSAL_write(cbi->fd, offset,
						arg->iov[i].iov_len,
						arg->iov[i].iov_base,
						&io_amount, &arg->fsal_stable,
						cbi->export_fd);
		else
			status = GPFSFSAL_read(cbi->fd, offset,
					       arg->iov[i].iov_len,
					       arg->iov[i].iov_base,
					       &io_amount, &arg->end_of_file,
					       cbi->export_fd);
		if (FSAL_IS_ERROR(status))
			break;
		offset += arg->iov[i].iov_len;
		arg->io_amount += io_amount;
	}
	cbi->status = status;
	gpfs_uring_release(cbi);
}

static uint64_t gpfs_uring_iov_offset(struct fsal_io_arg *arg, int index)
{
	uint64_t offset = arg->offset;
	int i;

	for (i = 0; i < index; i++)
		offset += arg->iov[i].iov_len;
	return offset;
}

static bool gpfs_uring_queue(struct fsal_obj_handle *obj_hdl, bool bypass,
			     fsal_async_cb done_cb, struct fsal_io_arg *arg,
			     void *caller_arg, bool is_write)
{
	struct gpfs_fsal_export *exp = NULL;
	struct gpfs_fsal_obj_handle *myself;
	struct gpfs_uring_cbi *cbi;
	struct fsal_fd *out_fd = NULL;
	fsal_status_t status;
	int armed = 0;
	int i;
	bool stable;

	if (!gpfs_uring_wanted(arg, is_write, &exp))
		return false;
	if (arg->offset > INT64_MAX) {
		arg->io_amount = 0;
		done_cb(obj_hdl, posix2fsal_status(EINVAL), arg, caller_arg);
		return true;
	}
	for (i = 0; i < arg->iov_count; i++) {
		if (arg->iov[i].iov_len > UINT32_MAX)
			return false;
	}
	if (gsh_uring_ensure(exp->io_uring_queue_depth) != 0)
		return false;

	cbi = gsh_calloc(1,
			 sizeof(*cbi) +
				 (size_t)arg->iov_count * sizeof(cbi->sqe[0]),
			 MEM_COMP_FSAL);
	pthread_mutex_init(&cbi->lock, NULL);
	cbi->arg = arg;
	cbi->exp = op_ctx->ctx_export;
	cbi->fsal_export = op_ctx->fsal_export;
	cbi->obj_hdl = obj_hdl;
	cbi->done_cb = done_cb;
	cbi->caller_arg = caller_arg;
	cbi->export_fd = exp->export_fd;
	cbi->is_write = is_write;
	cbi->want_stable = is_write && arg->fsal_stable;
	cbi->iov_count = arg->iov_count;
	cbi->status = fsalstat(ERR_FSAL_NO_ERROR, 0);
	init_fsal_fd(&cbi->temp_fd.fsal_fd, FSAL_FD_TEMP, op_ctx->fsal_export);
	cbi->temp_fd.fd = -1;
	for (i = 0; i < arg->iov_count; i++) {
		cbi->sqe[i].op.complete = gpfs_uring_on_cqe;
		cbi->sqe[i].cbi = cbi;
		cbi->sqe[i].index = i;
		cbi->sqe[i].len = arg->iov[i].iov_len;
	}

	myself = container_of(obj_hdl, struct gpfs_fsal_obj_handle, obj_handle);
	status = fsal_start_io(&out_fd, obj_hdl, &myself->u.file.fd.fsal_fd,
			       &cbi->temp_fd.fsal_fd, arg->state,
			       is_write ? FSAL_O_WRITE : FSAL_O_READ, false,
			       NULL, bypass, &myself->u.file.share);
	if (FSAL_IS_ERROR(status)) {
		LogFullDebug(COMPONENT_FSAL, "fsal_start_io failed returning %s",
			     fsal_err_txt(status));
		cbi->status = status;
		done_cb(obj_hdl, status, arg, caller_arg);
		pthread_mutex_destroy(&cbi->lock);
		gsh_free(cbi, MEM_COMP_FSAL);
		return true;
	}
	cbi->out_fd = out_fd;
	cbi->fd = container_of(out_fd, struct gpfs_fd, fsal_fd)->fd;
	arg->io_amount = 0;
	stable = cbi->want_stable;

	fsal_set_credentials(&op_ctx->creds);
	for (i = 0; i < arg->iov_count; i++) {
		int rc;

		rc = gsh_uring_submit_rw(cbi->fd, is_write,
					 arg->iov[i].iov_base,
					 (unsigned)arg->iov[i].iov_len,
					 gpfs_uring_iov_offset(arg, i), stable,
					 &cbi->sqe[i].op);
		if (rc != 0) {
			cbi->force_error = EIO;
			LogDebug(COMPONENT_FSAL,
				 "GPFS io_uring: submit failed (%d); ioctl fallback for the rest",
				 rc);
			break;
		}
		cbi->sqe[i].armed = true;
		armed++;
	}
	fsal_restore_ganesha_credentials();

	if (armed == 0) {
		LogDebug(COMPONENT_FSAL,
			 "GPFS io_uring: no SQE submitted, using ioctl");
		gpfs_uring_ioctl_fallback(cbi);
		return true;
	}
	if (armed < arg->iov_count)
		cbi->force_error = EIO;

	pthread_mutex_lock(&cbi->lock);
	cbi->armed = armed;
	cbi->posted = true;
	if (gpfs_uring_claim_locked(cbi)) {
		pthread_mutex_unlock(&cbi->lock);
		gpfs_uring_finish(cbi);
		return true;
	}
	pthread_mutex_unlock(&cbi->lock);
	return true;
}

bool gpfs_uring_queue_read(struct fsal_obj_handle *obj_hdl, bool bypass,
			   fsal_async_cb done_cb, struct fsal_io_arg *read_arg,
			   void *caller_arg)
{
	return gpfs_uring_queue(obj_hdl, bypass, done_cb, read_arg, caller_arg,
				false);
}

bool gpfs_uring_queue_write(struct fsal_obj_handle *obj_hdl, bool bypass,
			    fsal_async_cb done_cb,
			    struct fsal_io_arg *write_arg, void *caller_arg)
{
	return gpfs_uring_queue(obj_hdl, bypass, done_cb, write_arg, caller_arg,
				true);
}

void gpfs_uring_resume(struct fsal_obj_handle *obj_hdl,
		       fsal_async_cb done_cb, struct fsal_io_arg *arg,
		       void *caller_arg)
{
	struct gpfs_uring_cbi *cbi = arg->cbi;

	(void)obj_hdl;
	arg->fsal_resume = FSAL_NORESUME;
	arg->cbi = NULL;
	if (cbi == NULL) {
		LogMajor(COMPONENT_FSAL, "GPFS io_uring: resume without cbi");
		return;
	}
	/* MDCACHE replaces its callback wrapper when it resumes I/O. */
	cbi->done_cb = done_cb;
	cbi->caller_arg = caller_arg;
	gpfs_uring_release(cbi);
}
