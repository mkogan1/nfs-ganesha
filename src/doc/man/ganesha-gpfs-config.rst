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
    descriptor opened for that file, using one vectored operation per
    request. Requests exceeding the vector limits use the ioctl path.
    The default remains the
    OPENHANDLE_READ_BY_FD / OPENHANDLE_WRITE_BY_FD ioctl. READ_PLUS
    stays on the ioctl path. If the ring cannot be created, I/O uses
    the ioctl path.
    A stable write (the client asked for DATA_SYNC or FILE_SYNC, or
    NFS_Commit forced it) uses RWF_SYNC unless shared sync is enabled.
**io_uring_queue_depth(uint32, range 128 to 4096, default 128)**
    Maximum outstanding operations per NFS worker, rounded up to a power
    of two. Operations count until their callbacks return; submission waits
    at this limit. The first successful attachment fixes the worker's limit.
    Up to four shared owners submit and complete I/O. A newly created owner
    uses the attaching worker's queue depth, reserving one ring entry for
    its wakeup poll. Workers attached later share that ring's size.
    Owners batch up to 32 consecutive requests with matching credentials;
    partial batches are flushed immediately by default. Client credentials and groups
    are copied before queueing. Stable replies always wait for full synchronization.
    SINGLE_ISSUER, COOP_TASKRUN and DEFER_TASKRUN are used when supported,
    with fallback to ordinary ring setup on older kernels. Where supported,
    each owner limits its bounded io-wq pool to eight workers.

**io_uring_sync_batch_size(uint32, range 1 to 32, default 1)**
    Opt in to shared synchronization for stable writes. A value of 1 retains
    per-write RWF_SYNC. Larger values combine consecutive queued stable writes
    with the same held file descriptor, credentials, and batching policy on
    one owner. All writes complete before one full fsync is submitted; stable
    replies wait for its completion. A sync failure fails every otherwise
    successful write in that group. Different descriptors, including separate
    opens of the same file, are not combined. Singletons and group-allocation
    failures retain RWF_SYNC. This requires io_uring=true.
**io_uring_sync_delay_us(uint32, range 0 to 1000, default 0)**
    Maximum collection interval in microseconds from the first queued write
    when shared sync is enabled. Zero groups only requests already queued.
    Collection can overlap outstanding kernel I/O. Completions and ready syncs
    are processed without waiting for collection to finish. A full batch or
    an incompatible queued request ends collection early; wakeups do not
    extend its deadline. Scheduler delays and queue backpressure may exceed
    the requested interval. These export settings require a restart.

See also
==============================
:doc:`ganesha-log-config <ganesha-log-config>`\(8)
:doc:`ganesha-core-config <ganesha-core-config>`\(8)
:doc:`ganesha-export-config <ganesha-export-config>`\(8)
