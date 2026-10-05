/* SPDX-License-Identifier: LGPL-3.0-or-later */
/**
 * @file vfs_io_uring.h
 * @brief FSAL_VFS caller for the shared io_uring ring.
 */
#ifndef VFS_IO_URING_H
#define VFS_IO_URING_H

#include "fsal.h"

bool vfs_uring_queue_read(struct fsal_obj_handle *obj_hdl, bool bypass,
			  fsal_async_cb done_cb, struct fsal_io_arg *read_arg,
			  void *caller_arg);
bool vfs_uring_queue_write(struct fsal_obj_handle *obj_hdl, bool bypass,
			   fsal_async_cb done_cb, struct fsal_io_arg *write_arg,
			   void *caller_arg);
void vfs_uring_resume(struct fsal_obj_handle *obj_hdl,
		       fsal_async_cb done_cb, struct fsal_io_arg *arg,
		       void *caller_arg);

#endif
