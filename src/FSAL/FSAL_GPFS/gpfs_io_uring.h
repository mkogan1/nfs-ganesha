/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file gpfs_io_uring.h
 * @brief FSAL_GPFS glue for the shared io_uring ring.
 *
 * The default data path remains the OPENHANDLE_READ_BY_FD /
 * OPENHANDLE_WRITE_BY_FD ioctl. io_uring is opt-in and uses the file
 * descriptor from GPFSFSAL_open. READ_PLUS (IO_SKIP_HOLE) stays on the ioctl.
 */
#ifndef GPFS_IO_URING_H
#define GPFS_IO_URING_H

#include "fsal.h"

bool gpfs_uring_queue_read(struct fsal_obj_handle *obj_hdl, bool bypass,
			   fsal_async_cb done_cb, struct fsal_io_arg *read_arg,
			   void *caller_arg);
bool gpfs_uring_queue_write(struct fsal_obj_handle *obj_hdl, bool bypass,
			    fsal_async_cb done_cb,
			    struct fsal_io_arg *write_arg, void *caller_arg);
void gpfs_uring_resume(struct fsal_obj_handle *obj_hdl,
		       fsal_async_cb done_cb, struct fsal_io_arg *arg,
		       void *caller_arg);

#endif
