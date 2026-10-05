/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file vfs_io_uring.c
 * @brief FSAL_VFS caller for the shared io_uring ring.
 *
 * One scalar or vectored SQE matches vfs_read2 / vfs_write2. A stable write
 * uses RWF_SYNC instead of a following fsync. READ_PLUS is left to the
 * existing ENOTSUP path in vfs_read2.
 */
#include "config.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/uio.h>
#include <unistd.h>
#include "fsal.h"
#include "fsal_convert.h"
#include "FSAL/fsal_commonlib.h"
#include "abstract_mem.h"
#include "export_mgr.h"
#include "gsh_io_uring.h"
#include "vfs_methods.h"
#include "vfs_io_uring.h"

struct vfs_uring_cbi {
	struct gsh_uring_op op;
	struct fsal_io_arg *arg;
	struct gsh_export *exp;
	struct fsal_export *fsal_export;
	struct fsal_obj_handle *obj_hdl;
	fsal_async_cb done_cb;
	void *caller_arg;
	struct vfs_fd temp_fd;
	struct fsal_fd *out_fd;
	int fd;
	bool is_write;
	bool want_stable;
	bool posted;
	bool finished;
	bool have_res;
	int res;
	fsal_status_t status;
	pthread_mutex_t lock;
};

static bool vfs_uring_wanted(struct fsal_io_arg *arg,
			     struct vfs_fsal_export **exp_out)
{
	struct vfs_fsal_export *exp;

	if (gsh_uring_in_reaper())
		return false;
	if (arg->info != NULL)
		return false;
	if (arg->iov == NULL || arg->iov_count <= 0)
		return false;
	if (op_ctx == NULL || op_ctx->fsal_export == NULL)
		return false;
	exp = container_of(op_ctx->fsal_export, struct vfs_fsal_export, export);
	if (!exp->use_io_uring)
		return false;
	*exp_out = exp;
	return true;
}

static void vfs_uring_apply_results(struct vfs_uring_cbi *cbi)
{
	struct fsal_io_arg *arg = cbi->arg;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };

	if (!cbi->have_res) {
		status = posix2fsal_status(EIO);
	} else if (cbi->res < 0) {
		status = posix2fsal_status(-cbi->res);
	} else {
		arg->io_amount = (size_t)cbi->res;
		if (!cbi->is_write)
			arg->end_of_file = (cbi->res == 0);
		else if (cbi->want_stable)
			arg->fsal_stable = true;
	}
	cbi->status = status;
}

static void vfs_uring_release(struct vfs_uring_cbi *cbi)
{
	fsal_status_t status2;
	struct fsal_obj_handle *obj = cbi->obj_hdl;
	struct vfs_fsal_obj_handle *myself;
	fsal_openflags_t was;

	status2 = fsal_complete_io(obj, cbi->out_fd);
	LogFullDebug(COMPONENT_FSAL, "fsal_complete_io returned %s",
		     fsal_err_txt(status2));
	if (cbi->arg->state == NULL) {
		myself = container_of(obj, struct vfs_fsal_obj_handle,
				      obj_handle);
		was = cbi->is_write ? FSAL_O_WRITE : FSAL_O_READ;
		update_share_counters_locked(obj, &myself->u.file.share, was,
					     FSAL_O_CLOSED);
	}
	cbi->done_cb(obj, cbi->status, cbi->arg, cbi->caller_arg);
	pthread_mutex_destroy(&cbi->lock);
	gsh_free(cbi, MEM_COMP_FSAL);
}

static void vfs_uring_finish_inline(struct vfs_uring_cbi *cbi)
{
	vfs_uring_apply_results(cbi);
	vfs_uring_release(cbi);
}

static void vfs_uring_finish_on_reaper(struct vfs_uring_cbi *cbi)
{
	struct req_op_context ctx;

	vfs_uring_apply_results(cbi);
	if (cbi->exp == NULL || cbi->fsal_export == NULL) {
		LogMajor(COMPONENT_FSAL,
			 "VFS io_uring: completion without export");
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
	vfs_uring_release(cbi);
	release_op_context();
}

static void vfs_uring_finish(struct vfs_uring_cbi *cbi)
{
	if (gsh_uring_in_reaper())
		vfs_uring_finish_on_reaper(cbi);
	else
		vfs_uring_finish_inline(cbi);
}

static bool vfs_uring_claim_locked(struct vfs_uring_cbi *cbi)
{
	bool run;

	run = cbi->posted && cbi->have_res && !cbi->finished;
	if (run)
		cbi->finished = true;
	return run;
}

static void vfs_uring_on_cqe(struct gsh_uring_op *op, int res)
{
	struct vfs_uring_cbi *cbi = container_of(op, struct vfs_uring_cbi, op);
	bool run;

	pthread_mutex_lock(&cbi->lock);
	cbi->res = res;
	cbi->have_res = true;
	run = vfs_uring_claim_locked(cbi);
	pthread_mutex_unlock(&cbi->lock);
	if (run)
		vfs_uring_finish(cbi);
}

static void vfs_uring_posix_fallback(struct vfs_uring_cbi *cbi)
{
	struct fsal_io_arg *arg = cbi->arg;
	ssize_t n;
	fsal_status_t status = { ERR_FSAL_NO_ERROR, 0 };

	if (cbi->is_write) {
		n = pwritev(cbi->fd, arg->iov, arg->iov_count, arg->offset);
		if (n == -1) {
			status = posix2fsal_status(errno);
		} else {
			arg->io_amount = (size_t)n;
			if (cbi->want_stable && fsync(cbi->fd) == -1)
				arg->fsal_stable = false;
		}
	} else {
		n = preadv(cbi->fd, arg->iov, arg->iov_count, arg->offset);
		if (arg->offset == (uint64_t)-1 || n == -1) {
			status = posix2fsal_status(errno);
		} else {
			arg->io_amount = (size_t)n;
			arg->end_of_file = (n == 0);
		}
	}
	cbi->status = status;
}

static int vfs_uring_submit(struct vfs_uring_cbi *cbi)
{
	struct fsal_io_arg *arg = cbi->arg;

	if (arg->iov_count == 1 && arg->iov[0].iov_len <= UINT32_MAX)
		return gsh_uring_submit_rw(cbi->fd, cbi->is_write,
					   arg->iov[0].iov_base,
					   (unsigned int)arg->iov[0].iov_len,
					   arg->offset, cbi->want_stable,
					   &cbi->op);
	return gsh_uring_submit_rwv(cbi->fd, cbi->is_write, arg->iov,
				    (unsigned int)arg->iov_count, arg->offset,
				    cbi->want_stable, &cbi->op);
}

static bool vfs_uring_queue(struct fsal_obj_handle *obj_hdl, bool bypass,
			    fsal_async_cb done_cb, struct fsal_io_arg *arg,
			    void *caller_arg, bool is_write)
{
	struct vfs_fsal_export *exp = NULL;
	struct vfs_fsal_obj_handle *myself;
	struct vfs_uring_cbi *cbi;
	struct fsal_fd *out_fd = NULL;
	fsal_status_t status;
	int rc;
	bool creds = false;

	if (!vfs_uring_wanted(arg, &exp))
		return false;
	if (arg->offset > INT64_MAX) {
		arg->io_amount = 0;
		done_cb(obj_hdl, posix2fsal_status(EINVAL), arg, caller_arg);
		return true;
	}
	if (gsh_uring_ensure(exp->io_uring_queue_depth) != 0)
		return false;

	cbi = gsh_calloc(1, sizeof(*cbi), MEM_COMP_FSAL);
	pthread_mutex_init(&cbi->lock, NULL);
	cbi->op.complete = vfs_uring_on_cqe;
	cbi->arg = arg;
	cbi->exp = op_ctx->ctx_export;
	cbi->fsal_export = op_ctx->fsal_export;
	cbi->obj_hdl = obj_hdl;
	cbi->done_cb = done_cb;
	cbi->caller_arg = caller_arg;
	cbi->is_write = is_write;
	cbi->want_stable = is_write && arg->fsal_stable;
	cbi->status = fsalstat(ERR_FSAL_NO_ERROR, 0);
	init_fsal_fd(&cbi->temp_fd.fsal_fd, FSAL_FD_TEMP, op_ctx->fsal_export);
	cbi->temp_fd.fd = -1;

	myself = container_of(obj_hdl, struct vfs_fsal_obj_handle, obj_handle);
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
	cbi->fd = container_of(out_fd, struct vfs_fd, fsal_fd)->fd;
	arg->io_amount = 0;

	if (is_write) {
		if (!vfs_set_credentials(&op_ctx->creds, obj_hdl->fsal)) {
			cbi->status = posix2fsal_status(EPERM);
			vfs_uring_release(cbi);
			return true;
		}
		creds = true;
	}
	rc = vfs_uring_submit(cbi);
	if (creds)
		vfs_restore_ganesha_credentials(obj_hdl->fsal);
	if (rc != 0) {
		LogDebug(COMPONENT_FSAL,
			 "VFS io_uring: submit failed (%d); using preadv/pwritev",
			 rc);
		if (is_write &&
		    !vfs_set_credentials(&op_ctx->creds, obj_hdl->fsal)) {
			cbi->status = posix2fsal_status(EPERM);
			vfs_uring_release(cbi);
			return true;
		}
		vfs_uring_posix_fallback(cbi);
		if (is_write)
			vfs_restore_ganesha_credentials(obj_hdl->fsal);
		vfs_uring_release(cbi);
		return true;
	}

	pthread_mutex_lock(&cbi->lock);
	cbi->posted = true;
	if (vfs_uring_claim_locked(cbi)) {
		pthread_mutex_unlock(&cbi->lock);
		vfs_uring_finish(cbi);
		return true;
	}
	pthread_mutex_unlock(&cbi->lock);
	return true;
}

bool vfs_uring_queue_read(struct fsal_obj_handle *obj_hdl, bool bypass,
			  fsal_async_cb done_cb, struct fsal_io_arg *read_arg,
			  void *caller_arg)
{
	return vfs_uring_queue(obj_hdl, bypass, done_cb, read_arg, caller_arg,
			       false);
}

bool vfs_uring_queue_write(struct fsal_obj_handle *obj_hdl, bool bypass,
			   fsal_async_cb done_cb, struct fsal_io_arg *write_arg,
			   void *caller_arg)
{
	return vfs_uring_queue(obj_hdl, bypass, done_cb, write_arg, caller_arg,
			       true);
}

void vfs_uring_resume(struct fsal_obj_handle *obj_hdl,
		       fsal_async_cb done_cb, struct fsal_io_arg *arg,
		       void *caller_arg)
{
	struct vfs_uring_cbi *cbi = arg->cbi;

	(void)obj_hdl;
	arg->fsal_resume = FSAL_NORESUME;
	arg->cbi = NULL;
	if (cbi == NULL) {
		LogMajor(COMPONENT_FSAL, "VFS io_uring: resume without cbi");
		return;
	}
	/* MDCACHE replaces its callback wrapper when it resumes I/O. */
	cbi->done_cb = done_cb;
	cbi->caller_arg = caller_arg;
	vfs_uring_release(cbi);
}
