.. SPDX-License-Identifier: LGPL-3.0-or-later

===================================================================
ganesha-gpfs-config -- NFS Ganesha GPFS Configuration File
===================================================================

.. program:: ganesha-gpfs-config


SYNOPSIS
==========================================================

| /etc/ganesha/gpfs.conf

DESCRIPTION
==========================================================

NFS-Ganesha install the following config file for GPFS FSAL:
| /etc/ganesha/gpfs.conf

This file lists GPFS specific config options.

GPFS {}
--------------------------------------------------------------------------------

**link_support(bool, default true)**

**symlink_support(bool, default true)**

**cansettime(bool, default true)**

**umask(mode, range 0 to 0777, default 0)**

**auth_xdev_export(bool, default false)**

**Delegations(enum, default read)**

  Possible values:
	None, read, write, readwrite, r, w, rw

**pnfs_file(bool, default false)**

**fsal_trace(bool, default true)**

**fsal_grace(bool, default false)**

EXPORT { FSAL {} }
--------------------------------------------------------------------------------
**io_uring(bool, default false)**
    Requires the daemon built with -DWITH_SYSTEM_LIBURING=ON (default OFF).
    Submit normal GPFS reads and writes through io_uring on the file
    descriptor opened for that file. The default remains the
    OPENHANDLE_READ_BY_FD / OPENHANDLE_WRITE_BY_FD ioctl. READ_PLUS
    stays on the ioctl path. If the ring cannot be created, I/O uses
    the ioctl path.
    A stable write (the client asked for DATA_SYNC or FILE_SYNC, or
    NFS_Commit forced it) is submitted with RWF_SYNC.
**io_uring_queue_depth(uint32, range 128 to 4096, default 128)**
    io_uring SQ/CQ entries for each NFS worker thread that uses this
    export. Rounded up to a power of two. This is the ring size. The
    first successful ring init on a thread latches it. The submitting
    thread waits when the SQ is full.

See also
==============================
:doc:`ganesha-log-config <ganesha-log-config>`\(8)
:doc:`ganesha-core-config <ganesha-core-config>`\(8)
:doc:`ganesha-export-config <ganesha-export-config>`\(8)
