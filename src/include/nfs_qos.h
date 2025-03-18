/* SPDX-License-Identifier: LGPL-3.0-or-later */
/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) 2025, IBM . All rights reserved.
 * Author: Deeraj Patil <deeraj.patil@ibm.com>
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.  see <http://www.gnu.org/licenses/
 *
 * ---------------------------------------
 */
#if ENABLE_QOS

#include <time.h>

#define IS_QOS_IO (1 << 0)
#define IS_QOS_IO_READ_BYPASS (1 << 1)
#define IS_QOS_COMPOUND_IO (1 << 3)
#define IS_QOS_IOPS_ACCOUNTED (1 << 4)

#define NON_RATELIMITING_IO 0
#define RATELIMITING_IO 1

#define QOS_NOT_ENABLED 0
#define QOS_PER_EXPORT_ENABLED 1
#define QOS_PER_CLIENT_ENABLED 2
#define QOS_PEREXPORT_PERCLIENT_ENABLED 3

#define QOS_TASK_ASYNC_NOT_SCHEDULED 0
#define QOS_TASK_ASYNC_SCHEDULED 1

/* QOS configuration values for bandwidth and iops */
#define QOS_MIN_BW (1024UL * 1024) /* 1 MBps */
#define QOS_MAX_BW (100UL * 1024 * 1024 * 1024) /* 100GBps */
#define QOS_DEFAULT_EXPORT_BW (2UL * 1024 * 1024 * 1024) /* 2GBps */
#define QOS_DEFAULT_CLIENT_BW (2UL * 1024 * 1024 * 1024) /* 2GBps */

#define QOS_MIN_IOPS (10) /* i.e 2.5 MBps worth of IO */
#define QOS_MAX_IOPS (4 * 1024 * 100UL) /* 4op per MB * GB* 100 = 100GBps  */
#define QOS_DEFAULT_EXPORT_IOPS (4 * 1024 * 2UL) /* 4op per MB * GB* 2 = 2GBps*/
#define QOS_DEFAULT_CLIENT_IOPS (4 * 1024 * 2UL) /* 4op per MB * GB* 2 = 2GBps*/

#define QOS_MIN_TOKENS (QOS_MIN_BW * 3600) /* i.e 1MB * 3600Sec i.e 3600MB/Hr*/
#define QOS_MAX_TOKENS (UINT64_MAX)
#define QOS_DEFAULT_TOKENS (QOS_MIN_BW * 3600 * 24) /* Min BW * Per day limit */

#define QOS_MIN_TOKENS_REFRESH_TIME (3600) /* PerHr */
#define QOS_MAX_REFRESH_TIME (UINT64_MAX)
#define QOS_DEF_TOKEN_REFRESH_TIME (3600 * 24) /* Per 24 hours */

struct qos_op_cb_arg {
	/* caller_data is mainly the write_data and read_data ptr*/
	void *caller_data;
	/* Distingiush the IO for ratelimiting IO or token exhausted IO */
	uint32_t ratecontrol;
};

/* Structure used for handling the BW and token exhausted clients */
typedef struct timer_entry {
	/*  Epirt time of this IO */
	uint64_t expiry;
	/* Size of the IO, required for BW calculation */
	uint64_t size;
	/* Callback function to call on timer expiry */
	void (*callback)(void *);
	/* Call back arg : struct qos_op_cb_arg  */
	void *args;
	struct timer_entry *next;
} timer_entry_t;

/* Currently only NFS4 read IO and write IO are tapped */
enum qos_operation_type { QOS_READ, QOS_WRITE };

/* QOS can be enabled for PS, PC or PEPC */
enum qos_class_type { QOS_EXPORT, QOS_CLIENT, QOS_PEPC };

/* Structure used on token exhausted by client */
typedef struct qos_client_entry {
	sockaddr_t *client_addr;
	unsigned int num_ios_waiting;
	bool epoll_disabled;
	timer_entry_t *io_waitlist_qos;
	SVCXPRT *rq_xprt;
	compound_data_t *data;
	struct qos_client_entry *next;
} qos_client_entry_t;

/* Actual struture which holds the accounting.
 * In case of combined, only write accounting structure
 * will be used */
typedef struct qos_bucket {
	pthread_mutex_t lock;
	uint32_t num_ios_waiting;

	/* BW conrtol, io wait queue */
	timer_entry_t *io_waitlist_qos_bc;
	uint64_t max_bw_allowed;
	uint64_t bw_ldct;
	uint64_t data_consumed;

	timer_entry_t *io_waitlist_qos_iops;
	uint64_t max_iops_allowed;
	uint64_t iops_ldct;
	uint64_t iops_consumed;

	uint64_t max_available_tokens;
	uint64_t token_ldct;
	uint64_t tokens_consumed;
	uint64_t tokens_renew_time; /*   In useconds */
} qos_bucket_t;

/* QOS client specific struture for accounting */
typedef struct QoS_perClient_Class {
	/* Structure belongs to which gsh_client session,
	 * and will be valid till OP_DESTROY/final session deletion
	 **/
	sockaddr_t *client_addr;
	/* Used in case of only PEPC for export->clients->next  */
	struct QoS_perClient_Class *next;
	/*  Used for waiting IO accounting in case of token exhaust */
	unsigned int num_ios_waiting;
	bool bw_enabled;
	bool iops_enabled;
	bool token_enabled;
	bool combined_rw_bw_control;
	bool combined_rw_iops_control;
	bool combined_rw_token_control;
	/* lock used for adding/removing clients, client_entries etc */
	pthread_mutex_t lock;
	struct qos_bucket read_bucket;
	struct qos_bucket write_bucket;
	/* Entry is used to store the IO's after token exausted */
	struct qos_client_entry *client_entries;
} qos_client_t;

typedef struct QoS_perExport_Class {
	unsigned int export_id;
	/*  Used for waiting IO accounting in case of token exhaust */
	unsigned int num_ios_waiting;
	bool bw_enabled;
	bool iops_enabled;
	bool token_enabled;
	bool combined_rw_bw_control;
	bool combined_rw_iops_control;
	bool combined_rw_token_control;
	/* lock used for adding/removing clients, client_entries etc */
	pthread_mutex_t lock;
	struct qos_bucket read_bucket;
	struct qos_bucket write_bucket;
	struct QoS_perClient_Class *clients;
	/* Entry is used to store the IO's after token exausted */
	struct qos_client_entry *client_entries;
} qos_export_t;

typedef struct qos_block_config {
	bool enable_qos;

	bool enable_tokens;
	bool enable_bw_control;
	bool enable_iops_control;

	bool combined_rw_bw_control;
	bool combined_rw_token_control;
	bool combined_rw_iops_control;
	int qos_type;

	uint64_t max_export_combined_bw;
	uint64_t max_client_combined_bw;
	uint64_t max_export_write_bw;
	uint64_t max_export_read_bw;
	uint64_t max_client_write_bw;
	uint64_t max_client_read_bw;

	uint64_t max_export_combined_iops;
	uint64_t max_client_combined_iops;
	uint64_t max_export_write_iops;
	uint64_t max_export_read_iops;
	uint64_t max_client_write_iops;
	uint64_t max_client_read_iops;

	uint64_t max_export_read_tokens;
	uint64_t max_export_write_tokens;
	uint64_t max_client_read_tokens;
	uint64_t max_client_write_tokens;
	uint64_t export_read_tokens_renew_time;
	uint64_t export_write_tokens_renew_time;
	uint64_t client_read_tokens_renew_time;
	uint64_t client_write_tokens_renew_time;
} qos_block_config_t;

extern qos_block_config_t qos_block_config;
extern struct config_block qos_core;
extern struct qos_block_config *g_qos_config;

/* Structured for Future Generic Class implementation
struct Qos_Class
{
   enum qos_entity_type type;
   union  {
	sockaddr_t *client_addr
	uint64_t exportid;
   }
   struct Qos_Class *next;
   pthread_mutex_t lock;
   unsigned int num_ios_waiting;
   struct qos_bucket read_bucket;
   struct qos_bucket write_bucket;
   struct Qos_Class *subclass;
   struct qos_client_entry *t_exhausted_client;
   void *private;
}
*/

void QoS_perExportInsert(struct gsh_export *export,
			 struct qos_block_config *qos_block);
void qos_free_mem(void *gsh_ptr, unsigned int qos_class_type);
void qos_drain_bw_ios(void *qos_class, unsigned int qos_class_type);
unsigned int QoS_Process(unsigned int size, void *caller_data,
			 compound_data_t *data, unsigned int op_type);
qos_client_t *pepc_get_client_from_list(qos_client_t *head,
					sockaddr_t *client_addr);
void copy_gsh_qos_mem(struct gsh_export *dest, struct gsh_export *src);
void nfs4_qos_write_cb(void *args);
void nfs4_qos_read_cb(void *args);
void nfs4_qos_compond_cb(void *args);
unsigned int QoS_Process_iops(compound_data_t *data);
void qos_init(void);
#endif
