.. SPDX-License-Identifier: LGPL-3.0-or-later

===================================================================
ganesha-vfs-config -- NFS Ganesha VFS Configuration File
===================================================================

.. program:: ganesha-vfs-config


SYNOPSIS
==========================================================

| /etc/ganesha/vfs.conf

DESCRIPTION
==========================================================

NFS-Ganesha installs the config example for VFS FSAL:
| /etc/ganesha/vfs.conf

This file lists VFS specific config options.

EXPORT { FSAL {} }
--------------------------------------------------------------------------------

Name(string, "vfs")
    Name of FSAL should always be vfs.

**pnfs(bool, default false)**
**io_uring(bool, default false)**
    Requires the daemon built with -DWITH_SYSTEM_LIBURING=ON (default OFF).
    Submit reads and writes through the shared io_uring ring.
    The default remains preadv/pwritev. A stable write uses
    RWF_SYNC instead of a following fsync. If the ring cannot
    be created, I/O stays on preadv/pwritev.
**io_uring_queue_depth(uint32, range 128 to 4096, default 128)**
    Ring entries for each NFS worker. Rounded up to a power of two.
    The first successful init on a thread latches the size.

fsid_type(enum)
	Possible values:
	None, One64, Major64, Two64, uuid, Two32, Dev,Device


VFS {}
--------------------------------------------------------------------------------

**link_support(bool, default true)**

**symlink_support(bool, default true)**

**cansettime(bool, default true)**

**maxread(uint64, range 512 to 64*1024*1024, default 64*1024*1024)**

**maxwrite(uint64, range 512 to 64*1024*1024, default 64*1024*1024)**

**umask(mode, range 0 to 0777, default 0)**

**auth_xdev_export(bool, default false)**

**only_one_user(bool, default false)**

See also
==============================
:doc:`ganesha-log-config <ganesha-log-config>`\(8)
:doc:`ganesha-core-config <ganesha-core-config>`\(8)
:doc:`ganesha-export-config <ganesha-export-config>`\(8)
