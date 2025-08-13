// SPDX-License-Identifier: LGPL-3.0-or-later
/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright CEA/DAM/DIF  (2008)
 * contributeur : Philippe DENIEL   philippe.deniel@cea.fr
 *                Thomas LEIBOVICI  thomas.leibovici@cea.fr
 *
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * ---------------------------------------
 */

/**
 * @file    nfs4_op_free_stateid.c
 * @brief   Routines used for managing the NFS4 COMPOUND functions.
 *
 * Routines used for managing the NFS4 COMPOUND functions.
 *
 *
 */
#include "config.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "hashtable.h"
#include "log.h"
#include "gsh_rpc.h"
#include "nfs4.h"
#include "nfs_core.h"
#include "sal_functions.h"
#include "nfs_proto_functions.h"
#include "nfs_proto_tools.h"
#include "gsh_list.h"

#include "gsh_lttng/gsh_lttng.h"
#if defined(USE_LTTNG) && !defined(LTTNG_PARSING)
#include "gsh_lttng/generated_traces/nfs4.h"
#endif

/**
 *
 * @brief The NFS4_OP_FREE_STATEID operation.
 *
 * This function implements the NFS4_OP_FREE_STATEID operation in
 * nfs4_Compound.
 *
 * @param[in]     op    Arguments for nfs4_op
 * @param[in,out] data  Compound request's data
 * @param[out]    resp  Results for nfs4_op
 *
 * @return per RFC5661 pp. 364-5
 *
 * @see nfs4_Compound
 */

enum nfs_req_result nfs4_op_free_stateid(struct nfs_argop4 *op,
					 compound_data_t *data,
					 struct nfs_resop4 *resp)
{
	FREE_STATEID4args *const arg_FREE_STATEID4
		__attribute__((unused)) = &op->nfs_argop4_u.opfree_stateid;
	FREE_STATEID4res *const res_FREE_STATEID4 =
		&resp->nfs_resop4_u.opfree_stateid;
	state_t *state;
	struct saved_export_context saved;
	struct gsh_export *export;
	struct fsal_obj_handle *obj;

	GSH_AUTO_TRACEPOINT(
		nfs4, op_free_stateid_start, TRACE_INFO,
		"FREE_STATEID arg: seqid={} other={}",
		arg_FREE_STATEID4->fsa_stateid.seqid,
		TP_BYTE_ARR(arg_FREE_STATEID4->fsa_stateid.other,
			    sizeof(arg_FREE_STATEID4->fsa_stateid.other) /
				    sizeof(arg_FREE_STATEID4->fsa_stateid
						   .other[0])));

	resp->resop = NFS4_OP_FREE_STATEID;

	if (data->minorversion == 0) {
		res_FREE_STATEID4->fsr_status = NFS4ERR_INVAL;
		return NFS_REQ_ERROR;
	}

	/* Check if the stateid being freed corresponds to a delegation that
	 * was previously revoked.
	 *
	 * If the stateid exists in the revoked_delegations_list, it means the
	 * server has already revoked that delegation and has been returning
	 * NFS4ERR_DELEG_REVOKED for any operations using it.
	 *
	 * FREE_STATEID in this case serves as the client's acknowledgment
	 * of revocation. We remove the stateid from the revoked list and
	 * clean up any remaining state objects so they no longer consume
	 * server resources.
	 */
	if (is_stateid_revoked(&arg_FREE_STATEID4->fsa_stateid)) {
		LogDebug(
			COMPONENT_STATE,
			"Stateid is revoked, handling FREE_STATEID for revoked delegation");

		/* Obtain the state pointer without error */
		state = nfs4_State_Get_Pointer(
			(char *)arg_FREE_STATEID4->fsa_stateid.other);

		/* Remove stateid from revoked delegation list that was at
		 * the time of revoke
		 */
		remove_revoked_stateid(&arg_FREE_STATEID4->fsa_stateid);

		if (state) {
			LogDebug(
				COMPONENT_STATE,
				"Revoked delegation state found in state table, type=%d",
				state->state_type);

			if (state->state_type == STATE_TYPE_DELEG) {
				/* Get object and export references for
				 * locking and context
				 */
				if (get_state_obj_export_owner_refs(
					    state, &obj, &export, NULL)) {
					save_op_context_export_and_set_export(
						&saved, export);

					/* Lock and delete state */
					STATELOCK_lock(obj);
					state_del_locked(state);
					STATELOCK_unlock(obj);

					obj->obj_ops->put_ref(obj);
					restore_op_context_export(&saved);
				} else {
					/* Unable to get export/obj, fallback:
					 * just delete state without lock
					 */
					LogDebug(
						COMPONENT_STATE,
						"Could not get export/object refs for revoked delegation state cleanup");
					state_del_locked(state);
				}
				dec_state_t_ref(state);
				res_FREE_STATEID4->fsr_status = NFS4_OK;
				GSH_AUTO_TRACEPOINT(
					nfs4, op_free_stateid_end, TRACE_INFO,
					"FREE_STATEID res: status=%d (revoked delegation freed)",
					res_FREE_STATEID4->fsr_status);
				return NFS_REQ_OK;
			}
			LogDebug(
				COMPONENT_STATE,
				"Revoked state found but type is not delegation (%d) returning BAD_STATEID",
				state->state_type);

			res_FREE_STATEID4->fsr_status = NFS4ERR_BAD_STATEID;
			return nfsstat4_to_nfs_req_result(
				res_FREE_STATEID4->fsr_status);
		}
		LogDebug(
			COMPONENT_STATE,
			"Revoked delegation stateid not found in state table, treating FREE_STATEID as success");

		/* Since the revoked stateid is no longer in the state table,
		 * treat FREE_STATEID as successful
		 */
		res_FREE_STATEID4->fsr_status = NFS4_OK;
		return NFS_REQ_OK;
	}

	res_FREE_STATEID4->fsr_status = nfs4_Check_Stateid(
		&arg_FREE_STATEID4->fsa_stateid, NULL, &state, data,
		STATEID_SPECIAL_CURRENT, 0, false, "FREE_STATEID");

	if (res_FREE_STATEID4->fsr_status != NFS4_OK)
		return NFS_REQ_ERROR;

	if (!get_state_obj_export_owner_refs(state, &obj, &export, NULL)) {
		/* If this happens, something is going stale, just return
		 * NFS4ERR_BAD_STATEID, whatever is going stale will become
		 * more apparent to the client soon...
		 */
		res_FREE_STATEID4->fsr_status = NFS4ERR_BAD_STATEID;
		dec_state_t_ref(state);
		return NFS_REQ_ERROR;
	}

	save_op_context_export_and_set_export(&saved, export);

	STATELOCK_lock(obj);
	if (state->state_type == STATE_TYPE_LOCK &&
	    glist_empty(&state->state_data.lock.state_locklist)) {
		/* At the moment, only return success for a lock stateid with
		 * no locks.
		 */
		/** @todo: Do we also have to handle other kinds of stateids?
		 */
		res_FREE_STATEID4->fsr_status = NFS4_OK;
		state_del_locked(state);
	} else {
		res_FREE_STATEID4->fsr_status = NFS4ERR_LOCKS_HELD;
	}
	STATELOCK_unlock(obj);

	dec_state_t_ref(state);
	obj->obj_ops->put_ref(obj);
	restore_op_context_export(&saved);

	GSH_AUTO_TRACEPOINT(nfs4, op_free_stateid_end, TRACE_INFO,
			    "FREE_STATEID res: status={}",
			    res_FREE_STATEID4->fsr_status);
	return nfsstat4_to_nfs_req_result(res_FREE_STATEID4->fsr_status);

} /* nfs41_op_free_stateid */

/**
 * @brief free memory allocated for FREE_STATEID result
 *
 * This function frees memory allocated for the NFS4_OP_FREE_STATEID
 * result.
 *
 * @param[in,out] resp nfs4_op results
 *
 */
void nfs4_op_free_stateid_Free(nfs_resop4 *resp)
{
	/* Nothing to be done */
}
