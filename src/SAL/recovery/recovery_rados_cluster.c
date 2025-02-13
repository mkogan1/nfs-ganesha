// SPDX-License-Identifier: LGPL-3.0-or-later
/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright 2018 Red Hat, Inc. and/or its affiliates.
 * Author: Jeff Layton <jlayton@redhat.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * recovery_rados_cluster: a clustered recovery backing store
 *
 * See ganesha-rados-cluster-design(8) for overall design and theory
 */

#include "config.h"
#include <netdb.h>
#include <rados/librados.h>
#include <rados_grace.h>
#include <urcu-bp.h>
#include "log.h"
#include "nfs_core.h"
#include "sal_functions.h"
#include "recovery_rados.h"

/* Use hostname as nodeid in cluster */
char *nodeid;
static bool takeover = false;
/* recovery rados object names for takeover */
static char object_takeover[NI_MAXHOST];
static char object_takeover_old[NI_MAXHOST];
/* recovery rados object name for IP based backend */
static char object_ipbased[NI_MAXHOST];
static uint64_t rados_watch_cookie;
static int addr_int; /* IP address in int format */
uint64_t cur, rec;

static void rados_grace_watchcb(void *arg, uint64_t notify_id, uint64_t handle,
				uint64_t notifier_id, void *data,
				size_t data_len)
{
	int ret;

	/* ACK it first, so we keep things moving along */
	ret = rados_notify_ack(rados_recov_io_ctx, rados_kv_param.grace_oid,
			       notify_id, rados_watch_cookie, NULL, 0);
	if (ret < 0)
		LogEvent(COMPONENT_CLIENTID, "rados_notify_ack failed: %d",
			 ret);

	/* Now kick the reaper to check things out */
	nfs_notify_grace_waiters();
	reaper_wake();
}

/* Convert an IP address from string format to int32 */
static int ip_str_to_int(char *ip_str)
{
	unsigned char cl_addrbuf[sizeof(struct in6_addr)];
	sockaddr_t sp;
	int addr = 0;

	if (inet_pton(AF_INET, ip_str, cl_addrbuf) == 1) {
		sp.ss_family = AF_INET;
		memcpy(&((struct sockaddr_in *)&sp)->sin_addr, cl_addrbuf,
		       sizeof(struct in_addr));
		addr = ntohl(((struct sockaddr_in *)&sp)->sin_addr.s_addr);
	} else if (inet_pton(AF_INET6, ip_str, cl_addrbuf) == 1) {
		sp.ss_family = AF_INET6;
		memcpy(&((struct sockaddr_in6 *)&sp)->sin6_addr, cl_addrbuf,
		       sizeof(struct in6_addr));
		void *ab =
			&(((struct sockaddr_in6 *)&sp)->sin6_addr.s6_addr[12]);
		addr = ntohl(*(uint32_t *)ab);
	} else {
		LogCrit(COMPONENT_CLIENTID, "Unable to validate the IP : %s",
			ip_str);
	}
	return addr;
}

static int rados_cluster_init(void)
{
	int ret, node_id;
	long maxlen = sysconf(_SC_HOST_NAME_MAX);

	nodeid = gsh_malloc(maxlen);

	/* If no nodeid is specified, then use the hostname */
	if (rados_kv_param.nodeid >= 0) {
		node_id = rados_kv_param.nodeid;
		/* check nodeid override with "I" option */
		if (g_nodeid >= 0)
			node_id = g_nodeid;
		ret = snprintf(nodeid, maxlen, "node%d", node_id);
		if (unlikely(ret >= maxlen)) {
			LogCrit(COMPONENT_CLIENTID, "node%d too long", node_id);
			ret = -ENAMETOOLONG;
			goto out_free_nodeid;
		} else if (unlikely(ret < 0)) {
			ret = errno;
			LogCrit(COMPONENT_CLIENTID,
				"Unexpected return from snprintf %d error %s",
				ret, strerror(ret));
			goto out_free_nodeid;
		}
	} else {
		ret = gethostname(nodeid, maxlen);
		if (ret) {
			ret = -errno;
			LogCrit(COMPONENT_CLIENTID, "gethostname failed: %d",
				errno);
			goto out_free_nodeid;
		}
	}

	/* Form the recovery object name if IP Based recovery is enabled */
	if (nfs_param.nfsv4_param.recovery_backend_ipbased) {
		if (g_node_vip) {
			addr_int = ip_str_to_int(g_node_vip);
			ret = snprintf(object_ipbased, maxlen, "ip_%d",
				       addr_int);
			if (unlikely(ret >= maxlen) || unlikely(ret < 0))
				LogCrit(COMPONENT_CLIENTID,
					"Error while creating object name (IP based)");
		} else {
			/* IP address not provided, failback to nodeid based
			 * recovery mechanism */
			nfs_param.nfsv4_param.recovery_backend_ipbased = false;
		}
	}

	ret = rados_kv_connect(&rados_recov_io_ctx, rados_kv_param.userid,
			       rados_kv_param.ceph_conf, rados_kv_param.pool,
			       rados_kv_param.namespace);
	if (ret < 0) {
		LogCrit(COMPONENT_CLIENTID,
			"Failed to connect to rados cluster: %d", ret);
		goto out_shutdown;
	}

	ret = rados_grace_member(rados_recov_io_ctx, rados_kv_param.grace_oid,
				 nodeid);
	if (ret < 0) {
		LogCrit(COMPONENT_CLIENTID,
			"Cluster membership check failed: %d", ret);
		goto out_shutdown;
	}

	/* FIXME: not sure about the 30s timeout value here */
	ret = rados_watch3(rados_recov_io_ctx, rados_kv_param.grace_oid,
			   &rados_watch_cookie, rados_grace_watchcb, NULL, 30,
			   NULL);
	if (ret < 0) {
		LogCrit(COMPONENT_CLIENTID,
			"Failed to set watch on grace db: %d", ret);
		goto out_shutdown;
	}

	LogEvent(COMPONENT_CLIENTID,
		 "rados-cluster recovery backend initialization complete");

	return 0;

out_shutdown:
	rados_kv_shutdown();
out_free_nodeid:
	gsh_free(nodeid);
	nodeid = NULL;
	return ret;
}

/* Try to delete old recovery db */
static void rados_cluster_end_grace(void)
{
	int ret;
	rados_write_op_t wop;
	struct gsh_refstr *old_oid;

	old_oid = rcu_xchg_pointer(&rados_recov_old_oid, NULL);
	if (!old_oid)
		return;

	ret = rados_grace_enforcing_off(rados_recov_io_ctx,
					rados_kv_param.grace_oid, nodeid, &cur,
					&rec);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to set grace off for %s: %d", nodeid, ret);

	wop = rados_create_write_op();
	rados_write_op_remove(wop);
	if (!takeover) {
		LogDebug(COMPONENT_CLIENTID, "Recovery object removed: %s",
			 old_oid->gr_val);
		ret = rados_write_op_operate(wop, rados_recov_io_ctx,
					     old_oid->gr_val, NULL, 0);
	} else {
		LogDebug(COMPONENT_CLIENTID, "Recovery object removed: %s",
			 object_takeover_old);
		ret = rados_write_op_operate(wop, rados_recov_io_ctx,
					     object_takeover_old, NULL, 0);
	}

	if (ret) {
		if (!takeover)
			LogEvent(COMPONENT_CLIENTID, "Failed to remove %s: %d",
				 old_oid->gr_val, ret);
		else
			LogEvent(COMPONENT_CLIENTID, "Failed to remove %s: %d",
				 object_takeover_old, ret);
	}

	synchronize_rcu();
	gsh_refstr_put(old_oid);
}

void rados_cluster_add_clid(nfs_client_id_t *clientid)
{
	struct gsh_refstr *recov_oid;
	char rec_obj[NI_MAXHOST];

	if (nfs_param.nfsv4_param.recovery_backend_ipbased) {
		/* Use IP based recovery DB for storing client info */
		(void)snprintf(rec_obj, NI_MAXHOST - 1, "rec-%16.16lx:ip_%d",
			       cur,
			       clientid->cid_client_record->cr_server_addr);
		rados_kv_add_clid_impl(clientid, rec_obj);
	} else {
		rcu_read_lock();
		recov_oid = gsh_refstr_get(rcu_dereference(rados_recov_oid));
		rcu_read_unlock();
		rados_kv_add_clid_impl(clientid, recov_oid->gr_val);
		gsh_refstr_put(recov_oid);
	}
}

void rados_cluster_rm_clid(nfs_client_id_t *clientid)
{
	struct gsh_refstr *recov_oid;
	char rec_obj[NI_MAXHOST];

	if (nfs_param.nfsv4_param.recovery_backend_ipbased) {
		/* Use IP based recovery DB for storing client info */
		(void)snprintf(rec_obj, NI_MAXHOST - 1, "rec-%16.16lx:ip_%d",
			       cur,
			       clientid->cid_client_record->cr_server_addr);
		rados_kv_rm_clid_impl(clientid, rec_obj);
	} else {
		rcu_read_lock();
		recov_oid = gsh_refstr_get(rcu_dereference(rados_recov_oid));
		rcu_read_unlock();
		rados_kv_rm_clid_impl(clientid, recov_oid->gr_val);
		gsh_refstr_put(recov_oid);
	}
}

static void set_recovery_object_for_takeover(nfs_grace_start_t *gsp)
{
	int ret;
	int take_addr;

	takeover = false;
	switch (gsp->event) {
	case EVENT_TAKE_IP:
		if (!nfs_param.nfsv4_param.recovery_backend_ipbased) {
			LogCrit(COMPONENT_CLIENTID,
				"No takeover, IP based recovery mechanism not enabled.");
			break;
		}
		take_addr = ip_str_to_int(gsp->ipaddr);
		ret = snprintf(object_takeover, sizeof(object_takeover),
			       "ip_%d", take_addr);
		if (unlikely(ret >= sizeof(object_takeover))) {
			LogCrit(COMPONENT_CLIENTID,
				"object_takeover too long %s_recov",
				gsp->ipaddr);
		} else if (unlikely(ret < 0)) {
			LogCrit(COMPONENT_CLIENTID, "snprintf %d error %s (%d)",
				ret, strerror(errno), errno);
		} else
			takeover = true;
		break;
	case EVENT_TAKE_NODEID:
		if (nfs_param.nfsv4_param.recovery_backend_ipbased) {
			LogCrit(COMPONENT_CLIENTID,
				"No takeover, Nodeid based recovery mechanism not enabled.");
			break;
		}
		ret = snprintf(object_takeover, sizeof(object_takeover),
			       "node%d", gsp->nodeid);
		if (unlikely(ret >= sizeof(object_takeover))) {
			LogCrit(COMPONENT_CLIENTID,
				"Recovery object name too long: node%d",
				gsp->nodeid);
		} else if (unlikely(ret < 0)) {
			LogCrit(COMPONENT_CLIENTID, "snprintf %d error %s (%d)",
				ret, strerror(errno), errno);
		} else
			takeover = true;
		break;
	default:
		LogWarn(COMPONENT_CLIENTID,
			"Recovery unknown/unsupported event %d", gsp->event);
		return;
	}
}

static void rados_cluster_read_clids(nfs_grace_start_t *gsp,
				     add_clid_entry_hook add_clid_entry,
				     add_rfh_entry_hook add_rfh_entry)
{
	int ret;
	size_t new_len, old_len;
	rados_write_op_t wop;
	struct gsh_refstr *recov_oid, *old_oid;
	struct pop_args args = {
		.add_clid_entry = add_clid_entry,
		.add_rfh_entry = add_rfh_entry,
	};

	if (gsp && (gsp->event != EVENT_JUST_GRACE))
		set_recovery_object_for_takeover(gsp);

	/* Start or join a grace period */
	ret = rados_grace_join(rados_recov_io_ctx, rados_kv_param.grace_oid,
			       nodeid, &cur, &rec, true);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID, "Failed to join grace period: %d",
			 ret);
		return;
	}

	/*
	 * Recovery db names are like "rec-cccccccccccccccc:hostname" OR
	 * "rec-cccccccccccccccc:node<nodeid>" OR
	 * "rec-cccccccccccccccc:ip_<addr_in_int>"
	 *
	 * "rec-" followed by epoch in 16 hex digits + nodeid.
	 */

	/* calculate the recovery objects length */
	if (nfs_param.nfsv4_param.recovery_backend_ipbased)
		new_len = 4 + 16 + 1 + strlen(object_ipbased) + 1;
	else
		new_len = 4 + 16 + 1 + strlen(nodeid) + 1;
	if (!takeover)
		old_len = new_len;
	else
		old_len = 4 + 16 + 1 + strlen(object_takeover) + 1;
	recov_oid = gsh_refstr_alloc(new_len);

	/* Create new recovery object for current epoch */
	if (nfs_param.nfsv4_param.recovery_backend_ipbased)
		(void)snprintf(recov_oid->gr_val, new_len, "rec-%16.16lx:%s",
			       cur, object_ipbased);
	else
		(void)snprintf(recov_oid->gr_val, new_len, "rec-%16.16lx:%s",
			       cur, nodeid);
	LogDebug(COMPONENT_CLIENTID, "New recovery object %s",
		 recov_oid->gr_val);
	gsh_refstr_get(recov_oid);
	rcu_set_pointer(&rados_recov_oid, recov_oid);

	wop = rados_create_write_op();
	rados_write_op_create(wop, LIBRADOS_CREATE_IDEMPOTENT, NULL);
	rados_write_op_omap_clear(wop);
	ret = rados_write_op_operate(wop, rados_recov_io_ctx, recov_oid->gr_val,
				     NULL, 0);
	gsh_refstr_put(recov_oid);
	rados_release_write_op(wop);
	if (ret < 0) {
		LogEvent(COMPONENT_CLIENTID, "Failed to create recovery db");
		return;
	};

	/* Read from the recovery object targeted for reclaim */
	if (!takeover) {
		old_oid = gsh_refstr_alloc(old_len);
		if (nfs_param.nfsv4_param.recovery_backend_ipbased)
			(void)snprintf(old_oid->gr_val, old_len,
				       "rec-%16.16lx:%s", rec, object_ipbased);
		else
			(void)snprintf(old_oid->gr_val, old_len,
				       "rec-%16.16lx:%s", rec, nodeid);
		LogDebug(COMPONENT_CLIENTID,
			 "Recovery object for reclaim use %s", old_oid->gr_val);
		rcu_set_pointer(&rados_recov_old_oid, old_oid);
		ret = rados_kv_traverse(rados_ng_pop_clid_entry, &args,
					old_oid->gr_val);
	} else {
		(void)snprintf(object_takeover_old, old_len, "rec-%16.16lx:%s",
			       rec, object_takeover);
		LogDebug(COMPONENT_CLIENTID,
			 "Recovery object for reclaim use %s",
			 object_takeover_old);
		ret = rados_kv_traverse(rados_ng_pop_clid_entry, &args,
					object_takeover_old);
	}
	if (ret < 0)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to traverse recovery db: %d", ret);
}

static bool rados_cluster_try_lift_grace(void)
{
	int ret;

	ret = rados_grace_lift(rados_recov_io_ctx, rados_kv_param.grace_oid,
			       nodeid, &cur, &rec);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID, "Attempt to lift grace failed: %d",
			 ret);
		return false;
	}

	/* Non-zero rec means grace is still in force */
	return (rec == 0);
}

struct rados_cluster_kv_pairs {
	size_t slots; /* Current array size */
	size_t num; /* Count of populated elements */
	char **keys; /* Array of key strings */
	char **vals; /* Array of value blobs */
	size_t *lens; /* Array of value lengths */
};

/*
 * FIXME: Since each hash tree is protected by its own mutex, we can't ensure
 *        that we'll get an accurate count before allocating. For now, we just
 *        have a fixed-size cap of 1024 entries in the db, but we should allow
 *        there to be an arbitrary number of entries.
 */
#define RADOS_KV_STARTING_SLOTS 1024

static void rados_set_client_cb(struct rbt_node *pn, void *arg)
{
	struct hash_data *addr = RBT_OPAQ(pn);
	nfs_client_id_t *clientid = addr->val.addr;
	struct rados_cluster_kv_pairs *kvp = arg;
	char ckey[RADOS_KEY_MAX_LEN];

	/* FIXME: resize arrays in this case? */
	if (kvp->num >= kvp->slots) {
		LogEvent(COMPONENT_CLIENTID, "too many clients to copy!");
		return;
	}

	rados_kv_create_key(clientid, ckey, sizeof(ckey));

	kvp->keys[kvp->num] = gsh_strdup(ckey);
	kvp->vals[kvp->num] =
		rados_kv_create_val(clientid, &kvp->lens[kvp->num]);

	++kvp->num;
}

/**
 * @brief Start local grace period if we're in a global one
 *
 * In clustered setups, other machines in the cluster can start a new
 * grace period. Check for that and enter the grace period if so.
 */
static void rados_cluster_maybe_start_grace(void)
{
	int ret, i;
	size_t len;
	nfs_grace_start_t gsp = { .event = EVENT_JUST_GRACE };
	rados_write_op_t wop;
	struct gsh_refstr *recov_oid, *old_oid, *prev_recov_oid;
	char *keys[RADOS_KV_STARTING_SLOTS];
	char *vals[RADOS_KV_STARTING_SLOTS];
	size_t lens[RADOS_KV_STARTING_SLOTS];
	struct rados_cluster_kv_pairs kvp = { .slots = RADOS_KV_STARTING_SLOTS,
					      .num = 0,
					      .keys = keys,
					      .vals = vals,
					      .lens = lens };

	/* Fix up the strings */
	ret = rados_grace_epochs(rados_recov_io_ctx, rados_kv_param.grace_oid,
				 &cur, &rec);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID, "rados_grace_epochs failed: %d",
			 ret);
		return;
	}

	/* No grace period if rec == 0 */
	if (rec == 0)
		return;

	/*
	 * A new epoch has been started and a cluster-wide grace period has
	 * been requested. Make a new DB for "cur" that has all of the
	 * currently active clients in it.
	 */

	/* Allocate new oid string and xchg it into place */
	if (nfs_param.nfsv4_param.recovery_backend_ipbased)
		len = 4 + 16 + 1 + strlen(object_ipbased) + 1;
	else
		len = 4 + 16 + 1 + strlen(nodeid) + 1;
	recov_oid = gsh_refstr_alloc(len);

	/* Get an extra working reference of new string */
	gsh_refstr_get(recov_oid);

	/* Can't overrun and shouldn't return EOVERFLOW or EINVAL */
	if (nfs_param.nfsv4_param.recovery_backend_ipbased)
		(void)snprintf(recov_oid->gr_val, len, "rec-%16.16lx:%s", cur,
			       object_ipbased);
	else
		(void)snprintf(recov_oid->gr_val, len, "rec-%16.16lx:%s", cur,
			       nodeid);
	prev_recov_oid = rcu_xchg_pointer(&rados_recov_oid, recov_oid);

	old_oid = gsh_refstr_alloc(len);

	/* Can't overrun and shouldn't return EOVERFLOW or EINVAL */
	if (nfs_param.nfsv4_param.recovery_backend_ipbased)
		(void)snprintf(old_oid->gr_val, len, "rec-%16.16lx:%s", rec,
			       object_ipbased);
	else
		(void)snprintf(old_oid->gr_val, len, "rec-%16.16lx:%s", rec,
			       nodeid);
	old_oid = rcu_xchg_pointer(&rados_recov_old_oid, old_oid);

	synchronize_rcu();
	gsh_refstr_put(prev_recov_oid);
	if (old_oid)
		gsh_refstr_put(old_oid);

	/* Populate key/val/len arrays from confirmed client hash */
	hashtable_for_each(ht_confirmed_client_id, rados_set_client_cb, &kvp);

	/* Create new write op and package it up for callback */
	wop = rados_create_write_op();
	rados_write_op_create(wop, LIBRADOS_CREATE_IDEMPOTENT, NULL);
	rados_write_op_omap_clear(wop);
	rados_write_op_omap_set(wop, (char const *const *)keys,
				(char const *const *)vals, (const size_t *)lens,
				kvp.num);
	ret = rados_write_op_operate(wop, rados_recov_io_ctx, recov_oid->gr_val,
				     NULL, 0);
	gsh_refstr_put(recov_oid);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "rados_write_op_operate failed: %d", ret);

	rados_release_write_op(wop);

	/* Free copied strings */
	for (i = 0; i < kvp.num; ++i) {
		free(kvp.keys[i]);
		free(kvp.vals[i]);
	}

	/* Start a new grace period */
	nfs_start_grace(&gsp);
}

static void rados_cluster_shutdown(void)
{
	int ret;

	/*
	 * Request grace on clean shutdown to minimize the chance that we'll
	 * miss the window and the MDS kills off the old session.
	 *
	 * FIXME: only do this if our key is in the omap, and we have a
	 *        non-empty recovery db.
	 */
	ret = rados_grace_join(rados_recov_io_ctx, rados_kv_param.grace_oid,
			       nodeid, &cur, &rec, true);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to start grace period on shutdown: %d", ret);

	ret = rados_unwatch2(rados_recov_io_ctx, rados_watch_cookie);
	if (ret)
		LogEvent(COMPONENT_CLIENTID, "Failed to unwatch grace db: %d",
			 ret);

	rados_kv_shutdown();
	gsh_free(nodeid);
	nodeid = NULL;
}

static void rados_cluster_set_enforcing(void)
{
	int ret;

	ret = rados_grace_enforcing_on(rados_recov_io_ctx,
				       rados_kv_param.grace_oid, nodeid, &cur,
				       &rec);
	if (ret)
		LogEvent(COMPONENT_CLIENTID,
			 "Failed to set enforcing for %s: %d", nodeid, ret);
}

static bool rados_cluster_grace_enforcing(void)
{
	int ret;

	ret = rados_grace_enforcing_check(rados_recov_io_ctx,
					  rados_kv_param.grace_oid, nodeid);
	LogEvent(COMPONENT_CLIENTID, "%s: ret=%d", __func__, ret);
	return (ret == 0);
}

static bool rados_cluster_is_member(void)
{
	int ret = rados_grace_member(rados_recov_io_ctx,
				     rados_kv_param.grace_oid, nodeid);
	if (ret) {
		LogEvent(COMPONENT_CLIENTID,
			 "%s: %s is no longer a cluster member (ret=%d)",
			 __func__, nodeid, ret);
		return false;
	}
	return true;
}

static int rados_cluster_get_nodeid(char **pnodeid)
{
	*pnodeid = gsh_strdup(nodeid);
	return 0;
}

struct nfs4_recovery_backend rados_cluster_backend = {
	.recovery_init = rados_cluster_init,
	.recovery_shutdown = rados_cluster_shutdown,
	.recovery_read_clids = rados_cluster_read_clids,
	.end_grace = rados_cluster_end_grace,
	.add_clid = rados_cluster_add_clid,
	.rm_clid = rados_cluster_rm_clid,
	.add_revoke_fh = rados_kv_add_revoke_fh,
	.maybe_start_grace = rados_cluster_maybe_start_grace,
	.try_lift_grace = rados_cluster_try_lift_grace,
	.set_enforcing = rados_cluster_set_enforcing,
	.grace_enforcing = rados_cluster_grace_enforcing,
	.is_member = rados_cluster_is_member,
	.get_nodeid = rados_cluster_get_nodeid,
};

void rados_cluster_backend_init(struct nfs4_recovery_backend **backend)
{
	*backend = &rados_cluster_backend;
}
