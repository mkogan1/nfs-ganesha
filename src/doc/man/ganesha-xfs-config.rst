.. SPDX-License-Identifier: LGPL-3.0-or-later

===================================================================
ganesha-xfs-config -- NFS Ganesha XFS Configuration File
===================================================================

.. program:: ganesha-xfs-config


SYNOPSIS
==========================================================

    /etc/ganesha/xfs.conf

DESCRIPTION
==========================================================

NFS-Ganesha installs the config example for XFS FSAL:

    /etc/ganesha/xfs.conf

This file lists xfs specific config options.

EXPORT { FSAL {} }
--------------------------------------------------------------------------------

Name(string, "XFS")
    Name of FSAL should always be XFS.
**io_uring(bool, default false)**
    Requires the daemon built with -DWITH_SYSTEM_LIBURING=ON (default OFF).
    Same shared ring as FSAL_VFS. Default remains preadv/pwritev.
**io_uring_queue_depth(uint32, range 128 to 4096, default 128)**
    Same shared owners and per-worker outstanding-operation limit as VFS.
    Submission waits until a completion callback releases capacity.

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

XFS {}
--------------------------------------------------------------------------------
**link_support(bool, default true)**

**symlink_support(bool, default true)**

**cansettime(bool, default true)**

**maxread(uint64, range 512 to 64*1024*1024, default 64*1024*1024)**

**maxwrite(uint64, range 512 to 64*1024*1024, default 64*1024*1024)**

**umask(mode, range 0 to 0777, default 0)**

**auth_xdev_export(bool, default false)**

See also
==============================
:doc:`ganesha-log-config <ganesha-log-config>`\(8)
:doc:`ganesha-core-config <ganesha-core-config>`\(8)
:doc:`ganesha-export-config <ganesha-export-config>`\(8)
