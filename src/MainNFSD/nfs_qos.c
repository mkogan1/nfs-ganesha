// SPDX-License-Identifier: LGPL-3.0-or-later
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

/**
 * @file nfs_qos.c
 * @brief Routines used for managing the QOS.
 *
 * Routines used for managing the NFS4 QOS.
 *
 *
 */
#include "config.h"
#include "log.h"
#include "fsal.h"
#include "nfs_core.h"
#include "nfs_exports.h"
#include "sal_functions.h"
#include "nfs_proto_functions.h"
#include "nfs_proto_tools.h"
#include "nfs_convert.h"
#include "fsal_pnfs.h"
#include "server_stats.h"
#include "export_mgr.h"
#include "nfs_qos.h"
unsigned int qos_initalized;
typedef void (*qos_svc_rcb)(void *);
static void qos_token_exausted_deffer_task(void *ptr, void *caller_data,
					   compound_data_t *data,
					   unsigned int class_type,
					   unsigned int op_type);
static void qos_thread_init(void);
static void *qos_thread_func(void *arg);
static qos_client_entry_t *
get_and_insert_client_details(qos_client_entry_t **head, compound_data_t *data);
static timer_entry_t *create_timer_entry(uint64_t expiry,
					 void (*callback)(void *), void *args);
static void insert_timer_entry(timer_entry_t **head, timer_entry_t *new_entry);
static void remove_timer_entry(timer_entry_t **head,
			       timer_entry_t *entry_to_remove);
static inline bool check_bandwidth_and_delay(qos_bucket_t *bucket,
					     uint64_t bytes, void *caller_data,
					     unsigned int op_type);
static inline bool check_bandwidth_and_reschedule(qos_bucket_t *bucket,
						  uint64_t bytes,
						  void *caller_data,
						  unsigned int op_type);
static inline uint64_t get_time_in_usec(void);
static inline uint64_t get_time_future_useconds(uint64_t current,
						uint64_t seconds,
						uint64_t mseconds,
						uint64_t useconds);
static void setNode_pc(qos_client_t *node, sockaddr_t *client_addr,
		       struct qos_block_config *qos_block);
static inline qos_bucket_t *qos_get_bucket(void *entry, unsigned int class_type,
					   unsigned int op_type);
static inline qos_bucket_t *qos_get_token_bucket(void *entry,
						 unsigned int class_type,
						 unsigned int op_type);
static inline qos_bucket_t *
qos_get_bw_bucket(void *entry, unsigned int class_type, unsigned int op_type);
static inline qos_bucket_t *
qos_get_iops_bucket(void *entry, unsigned int class_type, unsigned int op_type);
static inline void qos_bw_bucket_deffer_task(qos_bucket_t *bucket,
					     void *caller_data,
					     uint64_t timeout, uint64_t size,
					     unsigned int op_type);
static inline void release_wait_ios(timer_entry_t **head,
				    unsigned int *counter1,
				    unsigned int *counter2);
static inline void pepc_free_client_list(qos_client_t **head);
qos_client_t *pepc_remove_client_from_list(qos_client_t **head,
					   sockaddr_t *client_addr);

qos_export_t *get_export_qos(struct gsh_export *gsh_export);
qos_client_t *get_client_qos(struct gsh_client *gsh_client);
static inline void resume_iops_pe(qos_export_t *export, unsigned int op_type);
static inline void resume_iops_pc(qos_client_t *client, unsigned int op_type);
static inline void pepc_reschedule_iops(qos_export_t *export,
					unsigned int op_type);
static inline void resume_iops_pepc(qos_export_t *export, unsigned int op_type);
#define THREAD_DELAY_NFS_ERR_DELAY_DEFAULT 15
#define THREAD_DELAY_NFS_ERR_DELAY_IMMED 1

/* Currently BW controlling using SYNC is disabled
 * This is compile time config */
#define BW_SYNC_ENABLE 0
#define BW_ASYNC_ENABLE !BW_SYNC_ENABLE

qos_block_config_t qos_block_config;
qos_block_config_t *g_qos_config = (qos_block_config_t *)&qos_block_config;

#define DELAY_MSEC 1000
#define USEC_IN_SEC (1000 * 1000)

#define BW_DELAY_MSEC 2
#define BW_DELAY_USEC (BW_DELAY_MSEC * 1000)

/* export level IO will be pushed down for future 5msec
 * Ensures even at heavy load, qos thread able to process enough IO's */
#define BW_EXPORT_FW_IO_SCHEDULE (BW_DELAY_USEC * 5)

/* Client level IO will be rescheduled to export bucket till
 * current time + 5 times the BW_EXPORT_FW_IO_SCHEDULE
 * This ensures even at load time enough IO's are schedules
 * from client bucket to export bucket in one iteration*/
#define BW_CLIENT_FW_IO_SCHEDULE (BW_EXPORT_FW_IO_SCHEDULE * 5)

/*  Indicates token refersh should happen every 1 sec */
#define TOKEN_REFRESH_DELAY (DELAY_MSEC / BW_DELAY_MSEC)

#define IOPS_DELAY_MSEC 5
#define IOPS_DELAY_USEC (BW_DELAY_MSEC * 1000)
#define IOPS_EXPORT_FW_IO_SCHEDULE (IOPS_DELAY_USEC * 5)
#define IOPS_CLIENT_FW_IO_SCHEDULE (IOPS_EXPORT_FW_IO_SCHEDULE * 5)
pthread_mutex_t g_qos_lock;

/**
 * Function to drain all Token I/Os for a given QoS class
 *
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] qos_class_type Type of the QoS class (export/client)
 */
static inline void qos_drain_token_ios(void *qos_class,
				       unsigned int qos_class_type)
{
	qos_client_entry_t *token_client = NULL;
	qos_client_entry_t *temp = NULL;
	uint32_t *num_ios_waiting = NULL;
	bool token_enabled = false;

	if (qos_class_type == QOS_EXPORT) {
		token_enabled = ((qos_export_t *)qos_class)->token_enabled;
		token_client = ((qos_export_t *)qos_class)->client_entries;
		num_ios_waiting =
			&(((qos_export_t *)qos_class)->num_ios_waiting);

		LogDebug(COMPONENT_QOS, "draining export:%d",
			 ((qos_export_t *)qos_class)->export_id);
	} else {
		token_enabled = ((qos_client_t *)qos_class)->token_enabled;
		token_client = ((qos_client_t *)qos_class)->client_entries;
		num_ios_waiting =
			&(((qos_client_t *)qos_class)->num_ios_waiting);

		LogDebug(COMPONENT_QOS, "draining client:%p",
			 ((qos_client_t *)qos_class)->client_addr);
	}
	if (token_enabled && token_client != NULL) {
		while (token_client != NULL) {
			temp = token_client->next;
			release_wait_ios(&(token_client->io_waitlist_qos),
					 num_ios_waiting,
					 &(token_client->num_ios_waiting));
			token_client = temp;
		}
	}
}

/**
 * Function to drain all BW I/Os for a given QoS class
 *
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] qos_class_type Type of the QoS class (export/client)
 */
void qos_drain_bw_ios(void *qos_class, unsigned int qos_class_type)
{
	bool bw_enabled = false;
	qos_bucket_t *rbucket =
		qos_get_bw_bucket(qos_class, qos_class_type, QOS_READ);
	qos_bucket_t *wbucket =
		qos_get_bw_bucket(qos_class, qos_class_type, QOS_WRITE);
	int dummy_counter = 0;

	if (qos_class_type == QOS_EXPORT) {
		bw_enabled = ((qos_export_t *)qos_class)->bw_enabled;
		((qos_export_t *)qos_class)->bw_enabled = 0;

		LogDebug(COMPONENT_QOS, "draining export:%d",
			 ((qos_export_t *)qos_class)->export_id);
	} else {
		bw_enabled = ((qos_client_t *)qos_class)->bw_enabled;
		((qos_client_t *)qos_class)->bw_enabled = 0;

		LogDebug(COMPONENT_QOS, "draining client:%p",
			 ((qos_client_t *)qos_class)->client_addr);
	}

	if (bw_enabled) {
		if (rbucket->io_waitlist_qos_bc != NULL) {
			release_wait_ios(&(rbucket->io_waitlist_qos_bc),
					 &(rbucket->num_ios_waiting),
					 &dummy_counter);
		}
		if (wbucket->io_waitlist_qos_bc != NULL) {
			release_wait_ios(&(wbucket->io_waitlist_qos_bc),
					 &(wbucket->num_ios_waiting),
					 &dummy_counter);
		}
	}
}

/**
 * Function to drain all IOPS I/Os for a given QoS class
 *
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] qos_class_type Type of the QoS class (export/client)
 */
void qos_drain_iops_ios(void *qos_class, unsigned int qos_class_type)
{
	bool iops_enabled = false;
	qos_bucket_t *rbucket =
		qos_get_iops_bucket(qos_class, qos_class_type, QOS_READ);
	qos_bucket_t *wbucket =
		qos_get_iops_bucket(qos_class, qos_class_type, QOS_WRITE);
	int dummy_counter = 0;

	if (qos_class_type == QOS_EXPORT) {
		iops_enabled = ((qos_export_t *)qos_class)->iops_enabled;
		((qos_export_t *)qos_class)->iops_enabled = 0;

		LogDebug(COMPONENT_QOS, "draining export:%d",
			 ((qos_export_t *)qos_class)->export_id);
	} else {
		iops_enabled = ((qos_client_t *)qos_class)->iops_enabled;
		((qos_client_t *)qos_class)->iops_enabled = 0;

		LogDebug(COMPONENT_QOS, "draining client:%p",
			 ((qos_client_t *)qos_class)->client_addr);
	}

	if (iops_enabled) {
		if (rbucket->io_waitlist_qos_iops != NULL) {
			release_wait_ios(&(rbucket->io_waitlist_qos_iops),
					 &(rbucket->num_ios_waiting),
					 &dummy_counter);
		}
		if (wbucket->io_waitlist_qos_iops != NULL) {
			release_wait_ios(&(wbucket->io_waitlist_qos_iops),
					 &(wbucket->num_ios_waiting),
					 &dummy_counter);
		}
	}
}

/**
 * Function to drain all I/Os for a given QoS class.
 *
 * @param [in] qos_class Pointer to the QoS export or client object.
 * @param [in] qos_class_type Type of the QoS class (export/client).
 */
void qos_drain_ios(void *qos_class, unsigned int qos_class_type)
{
	qos_drain_token_ios(qos_class, qos_class_type);
	qos_drain_bw_ios(qos_class, qos_class_type);
	qos_drain_iops_ios(qos_class, qos_class_type);
}

/**
 * Callback function to QoS free memory associated with a gsh_export structure.
 *
 * @param [in] export Pointer to the gsh_export structure.
 * @param [in] state Pointer to the sockaddr_t structure representing
 *			the client address.
 *
 * @return True if the iteration should continue, False otherwise.
 */
bool pepc_per_export_free_mem_cb(struct gsh_export *gsh_export, void *state)
{
	if (gsh_export == NULL)
		return true;

	qos_export_t *e_qos_class = gsh_export->qos_class;

	/* list is not populated */
	if (e_qos_class == NULL || e_qos_class->clients == NULL)
		return true;

	qos_client_t *c_qos_class = pepc_remove_client_from_list(
		&(e_qos_class->clients), (sockaddr_t *)state);

	if (c_qos_class != NULL) {
		LogDebug(COMPONENT_QOS, "freeing client:%d from export mem :%p",
			 gsh_export->qos_class->export_id, (sockaddr_t *)state);
		qos_drain_ios(c_qos_class, QOS_CLIENT);
		gsh_free(c_qos_class);
	} else {
		LogDebug(COMPONENT_QOS,
			 "Tried freeing client:%d from export mem :%p",
			 gsh_export->qos_class->export_id, (sockaddr_t *)state);
	}
	/* Continue the Iteration for next export */
	return true;
}

/**
 * Function to free memory associated with a QoS class and its buckets.
 *
 * @param [in] gsh_ptr Pointer to the gsh_export or gsh_client structure.
 * @param [in] qos_class_type Type of the QoS class (export/client).
 */
void qos_free_mem(void *gsh_ptr, unsigned int qos_class_type)
{
	struct gsh_export *export = gsh_ptr;
	struct gsh_client *client = gsh_ptr;

	if (qos_class_type == QOS_EXPORT) {
		if (export == NULL || export->qos_class == NULL)
			return;
	} else {
		if (client == NULL)
			return;
	}

	switch (g_qos_config->qos_type) {
	case QOS_PER_EXPORT_ENABLED:
		if (qos_class_type == QOS_EXPORT) {
			qos_drain_ios(export->qos_class, QOS_EXPORT);
			LogDebug(COMPONENT_QOS, "freeing export mem :%d",
				 export->qos_class->export_id);
			gsh_free(export->qos_class);
			if (export->qos_block)
				gsh_free(export->qos_block);
		}
		break;
	case QOS_PER_CLIENT_ENABLED:
		if (qos_class_type == QOS_CLIENT && client->qos_class != NULL) {
			LogDebug(COMPONENT_QOS, "freeing client mem :%p",
				 client->qos_class->client_addr);
			qos_drain_ios(client->qos_class, QOS_CLIENT);
			gsh_free(client->qos_class);
		}
		break;
	case QOS_PEREXPORT_PERCLIENT_ENABLED:
		if (qos_class_type == QOS_EXPORT) {
			qos_export_t *e_qos_class = export->qos_class;
			qos_client_t *c_qos_class = e_qos_class->clients;

			LogDebug(COMPONENT_QOS, "freeing export mem :%d",
				 export->qos_class->export_id);
			/* releasing waiting io's */
			while (c_qos_class != NULL) {
				qos_drain_ios(c_qos_class, QOS_CLIENT);
				c_qos_class = c_qos_class->next;
			}
			pepc_free_client_list(&e_qos_class->clients);
			qos_drain_ios(e_qos_class, QOS_EXPORT);
			gsh_free(export->qos_class);
			if (export->qos_block)
				gsh_free(export->qos_block);
		} else {
			LogDebug(COMPONENT_QOS,
				 "freeing client:%p from all export",
				 &(client->cl_addrbuf));
			foreach_gsh_export(pepc_per_export_free_mem_cb, false,
					   &(client->cl_addrbuf));
		}
		break;
	default:
		LogDebug(COMPONENT_QOS, " Something really wrong:%d",
			 g_qos_config->qos_type);
	}
}

/**
 * Function to copy memory from one gsh_export structure to another.
 *
 * @param [in] dest Pointer to the destination gsh_export structure.
 * @param [in] src Pointer to the source gsh_export structure.
 */
void copy_gsh_qos_mem(struct gsh_export *dest, struct gsh_export *src)
{
	if (g_qos_config->enable_qos == false)
		return;

	switch (g_qos_config->qos_type) {
	case QOS_PER_EXPORT_ENABLED:
		qos_block_config_t *new_values_pe = NULL;

		if (src->qos_block != NULL) {
			if (dest->qos_block != NULL) {
				memcpy(dest->qos_block, src->qos_block,
				       sizeof(qos_block_config_t));
				/*  Set the new values to the qos_class
				 *  using new dest->qos_block value */
				new_values_pe = dest->qos_block;
			} else {
				/* Refelect the new values to export
				 * and new struct will be freed
				 * newly provided qos_block in config file*/
				new_values_pe = src->qos_block;
			}
		} else {
			/* Refelect the global values to export*/
			new_values_pe = g_qos_config;
		}
		LogDebug(COMPONENT_QOS, "eid:%d sb:%p dp:%p gv:%p nv:%p",
			 dest->export_id, src->qos_block, dest->qos_block,
			 g_qos_config, new_values_pe);
		QoS_perExportInsert(dest, new_values_pe);

		break;
	case QOS_PER_CLIENT_ENABLED:
		LogDebug(COMPONENT_QOS, "NOT expected");
		break;
	case QOS_PEREXPORT_PERCLIENT_ENABLED:
		qos_block_config_t *new_values_pepc = NULL;
		qos_export_t *qos_export = NULL;
		qos_client_t *qos_client = NULL;

		if (src->qos_block != NULL) {
			if (dest->qos_block != NULL) {
				memcpy(dest->qos_block, src->qos_block,
				       sizeof(qos_block_config_t));
				/*  Set the new values to the qos_class using
				 *  new dest->qos_block value */
				new_values_pepc = dest->qos_block;
			} else {
				new_values_pepc = src->qos_block;
			}
		} else {
			new_values_pepc = g_qos_config;
		}
		LogDebug(COMPONENT_QOS, "eid:%d sb:%p dp:%p gv:%p nv:%p",
			 dest->export_id, src->qos_block, dest->qos_block,
			 g_qos_config, new_values_pepc);
		QoS_perExportInsert(dest, new_values_pepc);
		qos_export = dest->qos_class;
		PTHREAD_MUTEX_lock(&qos_export->lock);
		qos_client = qos_export->clients;
		while (qos_client) {
			setNode_pc(qos_client, qos_client->client_addr,
				   dest->qos_block);
			qos_client = qos_client->next;
		}
		PTHREAD_MUTEX_unlock(&qos_export->lock);
		break;
	default:
		LogDebug(COMPONENT_QOS, " Something really wrong:%d",
			 g_qos_config->qos_type);
	}
}

/**
 * Function to print the values of a qos_bucket_t structure.
 *
 * @param [in] bucket Pointer to the qos_bucket_t structure to be printed.
 */
static void print_bucket_values(qos_bucket_t *bucket)
{
	LogDebug(COMPONENT_QOS,
		 "wio:%d bw:%ld bw_ldct:%ld mat:%ld tc:%ld trt:%ld ltct:%ld",
		 bucket->num_ios_waiting, bucket->max_bw_allowed,
		 bucket->bw_ldct, bucket->max_available_tokens,
		 bucket->tokens_consumed, bucket->tokens_renew_time,
		 bucket->token_ldct);
	LogDebug(COMPONENT_QOS, "max_iops:%ld iops_ldct:%ld iops_consumed:%ld ",
		 bucket->max_iops_allowed, bucket->iops_ldct,
		 bucket->iops_consumed);
}
/**
 * Function to print the values of a QoS class (export/client).
 *
 * @param [in] qos_class Pointer to the QoS client or export structure.
 * @param [in] qos_class_type Type of the QoS class (export/client).
 * @param [in] str Unique string to be printed before the class details.
 */
static inline void print_class_values(void *qos_class,
				      unsigned int qos_class_type,
				      const char *str)
{
	if (qos_class_type == QOS_EXPORT) {
		qos_export_t *export = qos_class;

		LogDebug(COMPONENT_QOS, "%s SI:%d s_wio:%d", str,
			 export->export_id, export->num_ios_waiting);
		LogDebug(COMPONENT_QOS,
			 "bw_e:%d t_e:%d iops_e:%d c_bw:%d c_t:%d c_iops:%d",
			 export->bw_enabled, export->token_enabled,
			 export->iops_enabled, export->combined_rw_bw_control,
			 export->combined_rw_token_control,
			 export->combined_rw_iops_control);
		print_bucket_values(&(export->read_bucket));
		print_bucket_values(&(export->write_bucket));

	} else if (qos_class_type == QOS_CLIENT) {
		qos_client_t *client = qos_class;

		LogDebug(COMPONENT_QOS, "%s CI:%p s_wio:%d", str,
			 client->client_addr, client->num_ios_waiting);
		LogDebug(COMPONENT_QOS,
			 "bw_e:%d t_e:%d iops_e:%d c_bw:%d c_t:%d c_iops:%d",
			 client->bw_enabled, client->token_enabled,
			 client->iops_enabled, client->combined_rw_bw_control,
			 client->combined_rw_token_control,
			 client->combined_rw_iops_control);

		print_bucket_values(&(client->read_bucket));
		print_bucket_values(&(client->write_bucket));
	}
}

/**
 * Function to set the value of a token bucket.
 *
 * @param [in] bucket Pointer to the qos_bucket_t structure
 *		representing the token bucket.
 * @param [in] max_tokens Maximum number of tokens allowed in the bucket.
 * @param [in] tokens_renew_time Time interval for renewing tokens in seconds.
 */
static void set_bucket_value_token(qos_bucket_t *bucket, uint64_t max_tokens,
				   uint64_t tokens_renew_time)
{
	bucket->max_available_tokens = max_tokens;
	bucket->tokens_renew_time = (tokens_renew_time * 1000000);
}

/**
 * Function to set the value of a bandwidth bucket.
 *
 * @param [in] bucket Pointer to the qos_bucket_t structure
 *			representing the bandwidth bucket.
 * @param [in] max_bw Maximum bandwidth allowed in bytes per second.
 */
static void set_bucket_value_bw(qos_bucket_t *bucket, uint64_t max_bw)
{
	bucket->max_bw_allowed = max_bw;
}

/**
 * Function to set the value of an IOPS bucket.
 *
 * @param [in] bucket Pointer to the qos_bucket_t structure
 *			representing the IOPS bucket.
 * @param [in] max_iops Maximum IOPS allowed.
 */
static void set_bucket_value_iops(qos_bucket_t *bucket, uint64_t max_iops)
{
	bucket->max_iops_allowed = max_iops;
}

/**
 * Function to update Token values for a QoS class
 *
 * @param [in] class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 * @param [in] in  New values which are to be applied to token
 */
static void update_class_token_values(void *class, unsigned int class_type,
				      struct qos_block_config *in)
{
	qos_bucket_t *rbucket = qos_get_bucket(class, class_type, QOS_READ);
	qos_bucket_t *wbucket = qos_get_bucket(class, class_type, QOS_WRITE);
	bool *token_enabled = NULL;
	bool *combined_rw_token_control = NULL;
	uint64_t max_write_token = 0;
	uint64_t max_write_token_renew_time = 0;
	uint64_t max_read_token = 0;
	uint64_t max_read_token_renew_time = 0;

	if (class_type == QOS_EXPORT) {
		qos_export_t *qos_class = class;

		token_enabled = &(qos_class->token_enabled);
		combined_rw_token_control =
			&(qos_class->combined_rw_token_control);
		max_write_token = in->max_export_write_tokens;
		max_write_token_renew_time = in->export_write_tokens_renew_time;
		max_read_token = in->max_export_read_tokens;
		max_read_token_renew_time = in->export_read_tokens_renew_time;
	} else {
		qos_client_t *qos_class = class;

		token_enabled = &(qos_class->token_enabled);
		combined_rw_token_control =
			&(qos_class->combined_rw_token_control);
		max_write_token = in->max_client_write_tokens;
		max_write_token_renew_time = in->client_write_tokens_renew_time;
		max_read_token = in->max_client_read_tokens;
		max_read_token_renew_time = in->client_read_tokens_renew_time;
	}
	/* if true : Runtime enabling/updating of values */
	if ((g_qos_config->enable_tokens && in->enable_tokens)) {
		qos_drain_token_ios(class, class_type);
		if (in->combined_rw_token_control) {
			set_bucket_value_token(wbucket, max_write_token,
					       max_write_token_renew_time);
			*combined_rw_token_control = true;
		} else {
			set_bucket_value_token(wbucket, max_write_token,
					       max_write_token_renew_time);
			set_bucket_value_token(rbucket, max_read_token,
					       max_read_token_renew_time);
			*combined_rw_token_control = false;
		}
		*token_enabled = in->enable_tokens;
	} else {
		/* Runtime disabling of particular config */
		if (*token_enabled == true)
			qos_drain_token_ios(class, class_type);
	}
}

/**
 * Function to update BW values for a QoS class
 *
 * @param [in] class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 * @param [in] in  New values which are to be applied to BW
 */
static void update_class_bw_values(void *class, unsigned int class_type,
				   struct qos_block_config *in)
{
	qos_bucket_t *rbucket = qos_get_bucket(class, class_type, QOS_READ);
	qos_bucket_t *wbucket = qos_get_bucket(class, class_type, QOS_WRITE);
	bool *bw_enabled = NULL;
	bool *combined_rw_bw_control = NULL;
	uint64_t max_combined_bw = 0;
	uint64_t max_write_bw = 0;
	uint64_t max_read_bw = 0;
	int dummy_counter = UINT32_MAX;

	if (class_type == QOS_EXPORT) {
		qos_export_t *qos_class = class;

		bw_enabled = &(qos_class->bw_enabled);
		combined_rw_bw_control = &(qos_class->combined_rw_bw_control);
		max_combined_bw = in->max_export_combined_bw;
		max_write_bw = in->max_export_write_bw;
		max_read_bw = in->max_export_read_bw;
	} else {
		qos_client_t *qos_class = class;

		bw_enabled = &(qos_class->bw_enabled);
		combined_rw_bw_control = &(qos_class->combined_rw_bw_control);
		max_combined_bw = in->max_client_combined_bw;
		max_write_bw = in->max_client_write_bw;
		max_read_bw = in->max_client_read_bw;
	}

	/* if true : Runtime enabling/updating of values */
	if ((g_qos_config->enable_bw_control && in->enable_bw_control)) {
		if (in->combined_rw_bw_control) {
			set_bucket_value_bw(wbucket, max_combined_bw);
			/* Run time switching from 2 bucket to 1 bucket */
			if (rbucket->io_waitlist_qos_bc != NULL) {
				release_wait_ios(&(rbucket->io_waitlist_qos_bc),
						 &(rbucket->num_ios_waiting),
						 &dummy_counter);
			}
			*combined_rw_bw_control = true;
		} else {
			set_bucket_value_bw(wbucket, max_write_bw);
			set_bucket_value_bw(rbucket, max_read_bw);
			*combined_rw_bw_control = false;
		}
		*bw_enabled = in->enable_bw_control;
	} else {
		/* Runtime disabling of particular config */
		if (*bw_enabled == true)
			qos_drain_bw_ios(class, class_type);
	}
}

/**
 * Function to update IOPS values for a QoS class
 *
 * @param [in] class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 * @param [in] in  New values which are to be applied to iops
 */
static void update_class_iops_values(void *class, unsigned int class_type,
				     struct qos_block_config *in)
{
	qos_bucket_t *rbucket = qos_get_bucket(class, class_type, QOS_READ);
	qos_bucket_t *wbucket = qos_get_bucket(class, class_type, QOS_WRITE);
	bool *iops_enabled = NULL;
	bool *combined_rw_iops_control = NULL;
	uint64_t max_combined_iops = 0;
	uint64_t max_write_iops = 0;
	uint64_t max_read_iops = 0;
	int dummy_counter = UINT32_MAX;

	if (class_type == QOS_EXPORT) {
		qos_export_t *qos_class = class;

		iops_enabled = &(qos_class->iops_enabled);
		combined_rw_iops_control =
			&(qos_class->combined_rw_iops_control);
		max_combined_iops = in->max_export_combined_iops;
		max_write_iops = in->max_export_write_iops;
		max_read_iops = in->max_export_read_iops;
	} else {
		qos_client_t *qos_class = class;

		iops_enabled = &(qos_class->iops_enabled);
		combined_rw_iops_control =
			&(qos_class->combined_rw_iops_control);
		max_combined_iops = in->max_client_combined_iops;
		max_write_iops = in->max_client_write_iops;
		max_read_iops = in->max_client_read_iops;
	}
	/* if true : Runtime enabling/updating of values */
	if ((g_qos_config->enable_iops_control && in->enable_iops_control)) {
		if (in->combined_rw_iops_control) {
			set_bucket_value_iops(wbucket, max_combined_iops);
			/* Run time switching from 2 bucket to 1 bucket */
			if (rbucket->io_waitlist_qos_iops != NULL) {
				release_wait_ios(
					&(rbucket->io_waitlist_qos_iops),
					&(rbucket->num_ios_waiting),
					&dummy_counter);
			}
			*combined_rw_iops_control = true;
		} else {
			set_bucket_value_iops(wbucket, max_write_iops);
			set_bucket_value_iops(rbucket, max_read_iops);
			*combined_rw_iops_control = false;
		}
		*iops_enabled = in->enable_iops_control;
	} else {
		/* Runtime disabling of particular config */
		if (*iops_enabled == true)
			qos_drain_iops_ios(class, class_type);
	}
}

/**
 * Function to set values for a QoS class (export/client).
 *
 * @param [in] entry Pointer to the QoS export or client structure.
 * @param [in] class_type Type of the QoS class (export/client).
 * @param [in] in Pointer to the qos_block_config_t structure containing
 *			new/updated configuration information.
 */
static void set_class_values(void *entry, unsigned int class_type,
			     struct qos_block_config *in)
{
	if (in->enable_qos == false) {
		in->enable_tokens = false;
		in->enable_iops_control = false;
		in->enable_bw_control = false;
	}

	update_class_token_values(entry, class_type, in);
	update_class_bw_values(entry, class_type, in);
	update_class_iops_values(entry, class_type, in);
}

/**
 * Function to set values for a QoS export node.
 *
 * @param [in] node Pointer to qos_export_t structure representing the export.
 * @param [in] export_id ID of the export.
 * @param [in] qos_block Pointer to the qos_block_config_t structure containing
 *			new/updated configuration information.
 */
static void setNode_pe(qos_export_t *node, uint16_t export_id,
		       struct qos_block_config *qos_block)
{
	node->export_id = export_id;

	if (g_qos_config->enable_qos == false)
		return;

	LogDebug(COMPONENT_QOS, "Added new config for :%d", export_id);
	set_class_values(node, QOS_EXPORT, qos_block);
	print_class_values(node, QOS_EXPORT, "debugdp");
}

/**
 * Function to set values for a QoS client node.
 *
 * @param [in] node Pointer qos_client_t structure representing the client.
 * @param [in] client_addr Address of the client.
 * @param [in] qos_block Pointer to the qos_block_config_t structure containing
 *			new/updated configuration information.
 */
static void setNode_pc(qos_client_t *node, sockaddr_t *client_addr,
		       struct qos_block_config *qos_block)
{
	node->client_addr = client_addr;

	if (g_qos_config->enable_qos == false)
		return;

	LogDebug(COMPONENT_QOS, "Added new config for :%p", client_addr);
	set_class_values(node, QOS_CLIENT, qos_block);
	print_class_values(node, QOS_CLIENT, "debugdp");
}

/**
 * Function to insert a new QoS configuration for a gsh_export and
 *		is used while updating new values at runtime.
 *
 * @param [in] gsh_export Pointer to structure representing the gsh_export.
 * @param [in] qos_block Pointer to the qos_block_config_t structure
 *			new/updated containing configuration information.
 */
void QoS_perExportInsert(struct gsh_export *gsh_export,
			 struct qos_block_config *qos_block)
{
	struct qos_block_config *lqos_block = NULL;

	if (qos_block != NULL)
		lqos_block = qos_block;
	else
		lqos_block = g_qos_config;

	/*  Condition indicates this is new gsh_export, reexport or
	 *  run time enabled of QOS due to global config */
	if (gsh_export->qos_class == NULL) {
		qos_export_t *node = gsh_calloc(1, sizeof(qos_export_t));

		/* NULL Indicates QOS block is not populated
		 * i.e run time enabledment of QOS */
		if (gsh_export->qos_block == NULL) {
			qos_block_config_t *new_block;

			new_block = gsh_calloc(1, sizeof(qos_block_config_t));
			*new_block = *lqos_block;
			gsh_export->qos_block = new_block;
		}
		PTHREAD_MUTEX_init(&(node->lock), NULL);
		PTHREAD_MUTEX_init(&(node->read_bucket.lock), NULL);
		PTHREAD_MUTEX_init(&(node->write_bucket.lock), NULL);
		gsh_export->qos_class = node;
	} else {
		*(gsh_export->qos_block) = *lqos_block;
	}

	setNode_pe(gsh_export->qos_class, gsh_export->export_id, lqos_block);
}

/**
 * Function to allocate a new qos_client_t structure.
 *
 * @return Pointer to the newly allocated qos_client_t structure.
 */
qos_client_t *allocate_client(void)
{
	qos_client_t *node = gsh_calloc(1, sizeof(qos_client_t));

	PTHREAD_MUTEX_init(&(node->lock), NULL);
	PTHREAD_MUTEX_init(&(node->read_bucket.lock), NULL);
	PTHREAD_MUTEX_init(&(node->write_bucket.lock), NULL);
	return node;
}

/**
 * Function to allocate and initialize a new qos_client_t structure.
 *
 * @param [in] client_addr Address of the client.
 * @param [in] qos_block Pointer to qos_block_config_t structure containing
 *			configuration information.
 * @return Pointer to newly allocated and initialized qos_client_t structure.
 */
qos_client_t *pepc_allocate_and_init_client(sockaddr_t *client_addr,
					    struct qos_block_config *qos_block)
{
	qos_client_t *new_node = allocate_client();

	if (new_node)
		setNode_pc(new_node, client_addr, qos_block);

	return new_node;
}

/**
 * Function to add a new qos_client_t structure to the list.
 *
 * @param [in] head Pointer to the pointer of the head of the list.
 * @param [in] client Pointer to the qos_client_t structure to be added.
 */
void pepc_add_client_to_list(qos_client_t **head, qos_client_t *client)
{
	if (!client)
		return;

	client->next = *head;
	*head = client;
}

/**
 * Function to allocate, initialize and add new qos_client_t structure to list.
 *
 * @param [in] head Pointer to the pointer of the head of the list.
 * @param [in] client_addr Address of the client.
 * @param [in] qos_block Pointer to the qos_block_config_t structure
 *			containing configuration information.
 * @return Pointer to newly allocated and initialized qos_client_t structure.
 */
qos_client_t *pepc_alloc_init_add_client(qos_client_t **head,
					 sockaddr_t *client_addr,
					 struct qos_block_config *qos_block)
{
	qos_client_t *new_node =
		pepc_allocate_and_init_client(client_addr, qos_block);

	if (new_node)
		pepc_add_client_to_list(head, new_node);

	return new_node;
}

/**
 * Function to get a client from the list of clients for a export.
 *
 * @param [in] head Pointer to the head of the list of clients.
 * @param [in] client_addr Address of the client.
 * @return Pointer to retrieved qos_client_t structure, or NULL if not found.
 */
qos_client_t *pepc_get_client_from_list(qos_client_t *head,
					sockaddr_t *client_addr)
{
	qos_client_t *current = head;

	while (current != NULL) {
		if (current->client_addr == client_addr) {
			/* Client found */
			return current;
		}
		current = current->next;
	}
	/* Client Not Found */
	return NULL;
}

/**
 * Function to get/add a specific client from/to export list in pepc.
 *
 * @param [in] export Pointer to qos_export_t structure representing the export.
 * @param [in] client_addr Address of the client.
 * @return Pointer to retrieved qos_client_t structure, or NULL if not found.
 */
qos_client_t *pepc_get_client(qos_export_t *export, sockaddr_t *client_addr)
{
	/* Client Not Found */
	qos_client_t *client =
		pepc_get_client_from_list(export->clients, client_addr);
	if (client == NULL) {
		PTHREAD_MUTEX_lock(&export->lock);
		client =
			pepc_get_client_from_list(export->clients, client_addr);
		if (client == NULL)
			client = pepc_alloc_init_add_client(
				&(export->clients), client_addr,
				op_ctx->ctx_export->qos_block);
		PTHREAD_MUTEX_unlock(&export->lock);
	}

	return client;
}

/**
 * Function to remove a client from the list of clients for a export.
 *
 * @param [in] head Pointer to the pointer of the head of the list.
 * @param [in] client_addr Address of the client.
 * @return Pointer to the removed qos_client_t structure, or NULL if not found.
 */
qos_client_t *pepc_remove_client_from_list(qos_client_t **head,
					   sockaddr_t *client_addr)
{
	qos_client_t *current = *head;
	qos_client_t *prev = NULL;

	while (current != NULL) {
		if (current->client_addr == client_addr) {
			if (prev == NULL)
				/*  Removing Head */
				*head = current->next;
			else
				/* Skip current node */
				prev->next = current->next;

			return current;
		}
		prev = current;
		current = current->next;
	}
	return NULL;
}

/**
 * Function to free all the clients related to a export.
 *
 * @param [in] head Pointer to the pointer of the head of the list.
 */
static inline void pepc_free_client_list(qos_client_t **head)
{
	qos_client_t *current = *head;
	qos_client_t *next;

	while (current != NULL) {
		next = current->next;
		gsh_free(current);
		current = next;
	}
	head = NULL;
}

/**
 * Function to insert a new QoS configuration for a client.
 *
 * @param [in] qos_block Pointer to the qos_block_config_t structure containing
 *			configuration information.
 * @param [in] client Pointer to gsh_client structure representing the client.
 */
void QoS_perClientInsert(struct qos_block_config *qos_block,
			 struct gsh_client *client)
{
	qos_client_t *newNode = allocate_client();

	if (qos_block == NULL) {
		setNode_pc(newNode, &client->cl_addrbuf, g_qos_config);
		client->qos_class = newNode;
	} else {
		setNode_pc(newNode, &client->cl_addrbuf, qos_block);
		client->qos_class = newNode;
	}
}

/**
 * Function to get the current time in microseconds.
 *
 * @return Current time in microseconds.
 */
static inline uint64_t get_time_in_usec(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	/* Convert to uSeconds and return */
	return ((ts.tv_sec * 1000000) + (ts.tv_nsec / 1000));
}

/**
 * Function to get a qos_bucket_t structure for the specified entry,
 *			class type, and operation type.
 * This function should be used directly while updating the config values.
 *
 * @param [in] entry Pointer to the entry (qos_export_t or qos_client_t).
 * @param [in] class_type Type of the QoS class (export/client).
 * @param [in] op_type Type of the operation (read/write).
 * @return Pointer to qos_bucket_t forr the specified entry,
 *			class type, and operation type.
 */
static inline qos_bucket_t *qos_get_bucket(void *entry, unsigned int class_type,
					   unsigned int op_type)
{
	if (class_type == QOS_EXPORT || class_type == QOS_PEPC)
		return (op_type == QOS_READ) ?
			       &((qos_export_t *)entry)->read_bucket :
			       &((qos_export_t *)entry)->write_bucket;
	else
		return (op_type == QOS_READ) ?
			       &((qos_client_t *)entry)->read_bucket :
			       &((qos_client_t *)entry)->write_bucket;
}

/**
 * Function to get the token bucket for a given QoS class and operation type.
 * Functions should not be used while updating the conf values.
 *
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 * @param [in] op_type Type of the operation (read/write)
 * @return Pointer to the bandwidth bucket, or NULL if not found
 */
static inline qos_bucket_t *qos_get_token_bucket(void *qos_class,
						 unsigned int class_type,
						 unsigned int op_type)
{
	if (class_type == QOS_EXPORT || class_type == QOS_PEPC) {
		qos_export_t *export = qos_class;

		if (export->token_enabled == 0)
			return NULL;
		if (export->combined_rw_token_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(export, class_type, op_type);
	} else {
		qos_client_t *client = qos_class;

		if (client->token_enabled == 0)
			return NULL;
		if (client->combined_rw_token_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(client, class_type, op_type);
	}
}

/**
 * Function to get the bandwidth bucket for a given QoS class and operation type
 * Functions should not be used while updating the conf values.
 *
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 * @param [in] op_type Type of the operation (read/write)
 * @return Pointer to the bandwidth bucket, or NULL if not found
 */
static inline qos_bucket_t *qos_get_bw_bucket(void *qos_class,
					      unsigned int class_type,
					      unsigned int op_type)
{
	if (class_type == QOS_EXPORT || class_type == QOS_PEPC) {
		qos_export_t *export = qos_class;

		if (export->bw_enabled == 0)
			return NULL;
		if (export->combined_rw_bw_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(export, class_type, op_type);
	} else {
		qos_client_t *client = qos_class;

		if (client->bw_enabled == 0)
			return NULL;
		if (client->combined_rw_bw_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(client, class_type, op_type);
	}
}

/**
 * Function to get the IOPS bucket for a given QoS class and operation type
 * Functions should not be used while updating the conf values.
 *
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 * @param [in] op_type Type of the operation (read/write)
 * @return Pointer to the IOPS bucket, or NULL if not found
 */
static inline qos_bucket_t *qos_get_iops_bucket(void *qos_class,
						unsigned int class_type,
						unsigned int op_type)
{
	if (class_type == QOS_EXPORT || class_type == QOS_PEPC) {
		qos_export_t *export = qos_class;

		if (export->iops_enabled == 0)
			return NULL;
		if (export->combined_rw_iops_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(export, class_type, op_type);
	} else {
		qos_client_t *client = qos_class;

		if (client->iops_enabled == 0)
			return NULL;
		if (client->combined_rw_iops_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(client, class_type, op_type);
	}
}

/**
 * Function to check if there is enough token available in the bucket.
 *
 * @param [in] bucket Pointer to qos_bucket_t structure representing the token.
 * @param [in] request_size Size of the request for tokens.
 * @return True if the token is available, False otherwise.
 */
static inline bool qos_check_bucket_token_availability(qos_bucket_t *bucket,
						       uint64_t request_size)
{
	if (bucket->tokens_consumed <= bucket->max_available_tokens)
		return true;
	else
		return false;
}

/**
 * Function to consume tokens from the bucket.
 *
 * @param [in] bucket Pointer to qos_bucket_t structure representing the token.
 * @param [in] request_size Size of the request for tokens.
 */
static inline void qos_consume_bucket_token(qos_bucket_t *bucket,
					    uint64_t request_size)
{
	bucket->token_ldct = get_time_in_usec();
	bucket->tokens_consumed += request_size;
}

/**
 * Function to check if there is enough token available.
 *
 * @param [in] qos_class Pointer to the QoS client or export structure.
 * @param [in] request_size Size of the request for tokens.
 * @param [in] op_type Type of the operation (read/write).
 * @param [in] class_type Type of the QoS class (export/client).
 * @return True if token is consumed, False otherwise.
 */
static inline bool qos_check_token_availability(void *qos_class,
						uint64_t request_size,
						unsigned int op_type,
						unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return true;
	return qos_check_bucket_token_availability(bucket, request_size);
}

/**
 * Function to consume tokens from the bucket.
 *
 * @param [in] qos_class Pointer to the QoS client or export structure.
 * @param [in] request_size Size of the request for tokens.
 * @param [in] op_type Type of the operation (read/write).
 * @param [in] class_type Type of the QoS class (export/client).
 */
static inline void qos_consume_token(void *qos_class, uint64_t request_size,
				     unsigned int op_type,
				     unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return;
	return qos_consume_bucket_token(bucket, request_size);
}

/**
 * Function to control bandwidth for the bucket.
 *
 * @param [in] bucket Pointer to qos_bucket_t structure representing
 *			the bandwidth bucket.
 * @param [in] request_size Size of the request for bandwidth.
 * @param [in] op_type Type of the operation (read/write).
 * @param [in] caller_data Pointer to the caller data.
 * @return False if IO is recheduled and False if IO is not rescheduled.
 */
static inline bool qos_control_bucket_bw(qos_bucket_t *bucket,
					 uint64_t request_size,
					 unsigned int op_type,
					 void *caller_data)
{
	if (BW_SYNC_ENABLE &&
	    !check_bandwidth_and_delay(bucket, request_size, caller_data,
				       op_type)) {
		return true;
	} else if (BW_ASYNC_ENABLE &&
		   !check_bandwidth_and_reschedule(bucket, request_size,
						   caller_data, op_type)) {
		return false;
	} else {
		return true;
	}
}

/**
 * Function to control bandwidth for the QoS class.
 *
 * @param [in] qos_class Pointer to the QoS client or export structure.
 * @param [in] request_size Size of the request for bandwidth.
 * @param [in] op_type Type of the operation (read/write).
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] class_type Type of the QoS class (export/client).
 * @return True if bandwidth is controlled, False otherwise.
 */
static inline bool qos_control_bw(void *qos_class, uint64_t request_size,
				  unsigned int op_type, void *caller_data,
				  unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_bw_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return true;

	return qos_control_bucket_bw(bucket, request_size, op_type,
				     caller_data);
}

/**
 * Function to defer the task for bandwidth control.
 *
 * @param [in] qos_class Pointer to the QoS client or export structure.
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] size Size of the request.
 * @param [in] timeout Timeout for the task.
 * @param [in] op_type Type of the operation (read/write).
 * @param [in] class_type Type of the QoS class (export/client).
 */
static inline void qos_bw_deffer_task(void *qos_class, void *caller_data,
				      uint64_t size, uint64_t timeout,
				      unsigned int op_type,
				      unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_bw_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return;

	return qos_bw_bucket_deffer_task(bucket, caller_data, size, timeout,
					 op_type);
}

/**
 * Function to check if the QoS token is available and consume it if possible
 * along with it takecare of BW calculation also
 *
 * @param [in] class_ptr Pointer to the QoS export object
 * @param [in] request_size Size of the I/O request in bytes
 * @param [in] op_type Type of the operation (read/write)
 * @param [in] caller_data Caller data passed to the callback function
 * @param [in] data Compound data associated with the I/O request
 * @param [in] class_type Type of the QoS class (export/client/pepc)
 * @return True if the token is available and consumed, False otherwise
 */

static bool qos_check_pe(void *class_ptr, uint64_t request_size,
			 unsigned int op_type, void *caller_data,
			 compound_data_t *data, unsigned int class_type)
{
	qos_export_t *qos_class = class_ptr;

	PTHREAD_MUTEX_lock(&qos_class->lock);
	if (!qos_check_token_availability(qos_class, request_size, op_type,
					  QOS_EXPORT)) {
		qos_token_exausted_deffer_task(qos_class, caller_data, data,
					       QOS_EXPORT, op_type);
		PTHREAD_MUTEX_unlock(&qos_class->lock);
		return false;
	} else if (!qos_control_bw(qos_class, request_size, op_type,
				   caller_data, QOS_EXPORT)) {
		/*  Consume the ASYNC scheduled tokens */
		qos_consume_token(qos_class, request_size, op_type, QOS_EXPORT);
		PTHREAD_MUTEX_unlock(&qos_class->lock);
		return false;
	}
	qos_consume_token(qos_class, request_size, op_type, QOS_EXPORT);
	PTHREAD_MUTEX_unlock(&qos_class->lock);
	return true;
}

/**
 * Function to check if the QoS token is available and consume it if possible
 * along with it takecare of BW calculation also
 *
 * @param [in] class_ptr Pointer to the QoS Client object
 * @param [in] request_size Size of the I/O request in bytes
 * @param [in] op_type Type of the operation (read/write)
 * @param [in] caller_data Caller data passed to the callback function
 * @param [in] data Compound data associated with the I/O request
 * @param [in] class_type Type of the QoS class (export/client/pepc)
 * @return True if the token is available and consumed, False otherwise
 */
static bool qos_check_pc(void *class_ptr, uint64_t request_size,
			 unsigned int op_type, void *caller_data,
			 compound_data_t *data, unsigned int class_type)
{
	qos_client_t *qos_class = class_ptr;

	PTHREAD_MUTEX_lock(&qos_class->lock);
	if (!qos_check_token_availability(qos_class, request_size, op_type,
					  QOS_CLIENT)) {
		qos_token_exausted_deffer_task(qos_class, caller_data, data,
					       QOS_CLIENT, op_type);
		PTHREAD_MUTEX_unlock(&qos_class->lock);
		return false;
	} else if (!qos_control_bw(qos_class, request_size, op_type,
				   caller_data, QOS_CLIENT)) {
		qos_consume_token(qos_class, request_size, op_type, QOS_CLIENT);
		PTHREAD_MUTEX_unlock(&qos_class->lock);
		return false;
	}
	qos_consume_token(qos_class, request_size, op_type, QOS_CLIENT);
	PTHREAD_MUTEX_unlock(&qos_class->lock);
	return true;
}

/**
 * Function to check if the QoS token is available and consume it if possible
 * along with it takecare of deffering the task to client specific Queue for BW
 *
 * @param [in] class_ptr Pointer to the QoS export object
 * @param [in] request_size Size of the I/O request in bytes
 * @param [in] op_type Type of the operation (read/write)
 * @param [in] caller_data Caller data passed to the callback function
 * @param [in] data Compound data associated with the I/O request
 * @param [in] class_type Type of the QoS class (export/client/pepc)
 * @return True if the token is available and consumed, False otherwise
 */
static bool qos_check_pepc(void *class_ptr, uint64_t request_size,
			   unsigned int op_type, void *caller_data,
			   compound_data_t *data, unsigned int class_type)
{
	qos_export_t *e_qos_class = class_ptr;
	qos_client_t *c_qos_class =
		pepc_get_client(e_qos_class, &op_ctx->client->cl_addrbuf);

	int export_token_available = qos_check_token_availability(
		e_qos_class, request_size, op_type, QOS_EXPORT);
	int client_token_available = qos_check_token_availability(
		c_qos_class, request_size, op_type, QOS_CLIENT);

	/*  here for accounting info take exportlevel lock,
	 *  which will aloow us to handling the runtime disablement
	 *  and enablement of QOS BW and Token control.
	 *  IO Consumer thread will work on bucket locks */
	PTHREAD_MUTEX_lock(&e_qos_class->lock);
	if (!export_token_available) {
		qos_token_exausted_deffer_task(e_qos_class, caller_data, data,
					       QOS_EXPORT, op_type);
		PTHREAD_MUTEX_unlock(&e_qos_class->lock);
		return false;
	} else if (!client_token_available) {
		qos_token_exausted_deffer_task(c_qos_class, caller_data, data,
					       QOS_CLIENT, op_type);
		PTHREAD_MUTEX_unlock(&e_qos_class->lock);
		return false;
	} else {
		/*  consume the Tokens and schedule for ASYNC in the
		 *  client buckets, rescheuling of IO to export bucket
		 *  will happend later.
		 *  BW limits also decided later */
		qos_consume_token(e_qos_class, request_size, op_type,
				  QOS_EXPORT);
		qos_consume_token(c_qos_class, request_size, op_type,
				  QOS_CLIENT);
		if (c_qos_class->bw_enabled) {
			qos_bw_deffer_task(c_qos_class, caller_data,
					   request_size, get_time_in_usec(),
					   op_type, QOS_CLIENT);
			PTHREAD_MUTEX_unlock(&e_qos_class->lock);
			return false;
		} else {
			PTHREAD_MUTEX_unlock(&e_qos_class->lock);
			return true;
		}
	}
	PTHREAD_MUTEX_unlock(&e_qos_class->lock);
	return true;
}

/**
 * Function to process a export request for BW and Token.
 *
 * @param [in] size Size of the request.
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] data Pointer to the compound_data_t structure containing
 *			the request data.
 * @param [in] op_type Type of the operation (read/write).
 * @return 0 if successful, 1 otherwise.
 */
unsigned int QoS_Process_pe(unsigned int size, void *caller_data,
			    compound_data_t *data, unsigned int op_type)
{
	if (op_ctx->ctx_export->qos_class == NULL) {
		PTHREAD_MUTEX_lock(&g_qos_lock);
		if (op_ctx->ctx_export->qos_class == NULL)
			QoS_perExportInsert(op_ctx->ctx_export, g_qos_config);

		PTHREAD_MUTEX_unlock(&g_qos_lock);
	}
	if (!qos_check_pe(op_ctx->ctx_export->qos_class, size, op_type,
			  caller_data, data, QOS_EXPORT)) {
		return 1;
	}
	return 0;
}

/**
 * Function to process a client request for BW and Token.
 *
 * @param [in] size Size of the request.
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] data Pointer to the compound_data_t structure containing
 *			the request data.
 * @param [in] op_type Type of the operation (read/write).
 * @return 0 if successful, 1 otherwise.
 */
unsigned int QoS_Process_pc(unsigned int size, void *caller_data,
			    compound_data_t *data, unsigned int op_type)
{
	if (op_ctx->client->qos_class == NULL) {
		PTHREAD_MUTEX_lock(&g_qos_lock);
		/* Since this is QOS_PC, pass the global QOS values */
		if (op_ctx->client->qos_class == NULL)
			QoS_perClientInsert(g_qos_config, op_ctx->client);

		PTHREAD_MUTEX_unlock(&g_qos_lock);
	}
	if (!qos_check_pc(op_ctx->client->qos_class, size, op_type, caller_data,
			  data, QOS_CLIENT)) {
		return 1;
	}
	return 0;
}

/**
 * Function to process a pepc export request for BW and Token.
 *
 * @param [in] size Size of the request.
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] data Pointer to the compound_data_t structure containing
 *			the request data.
 * @param [in] op_type Type of the operation (read/write).
 * @return 0 if successful, 1 otherwise.
 */
unsigned int QoS_Process_pepc(unsigned int size, void *caller_data,
			      compound_data_t *data, unsigned int op_type)
{
	qos_export_t *export = op_ctx->ctx_export->qos_class;
	sockaddr_t *client_addr = &op_ctx->client->cl_addrbuf;

	if (export == NULL) {
		/* Execution reached here means QOS is enabled but
		 * export level qos block is not present,
		 * apply global conf to export
		 * There is possiblity of multiple IO's rushing
		 * into this function for this paticular export,
		 * so recheck before going for allocation*/
		PTHREAD_MUTEX_lock(&g_qos_lock);
		export = op_ctx->ctx_export->qos_class;
		if (export == NULL) {
			QoS_perExportInsert(op_ctx->ctx_export, g_qos_config);
			export = op_ctx->ctx_export->qos_class;
		}
		PTHREAD_MUTEX_unlock(&g_qos_lock);
	}
	/* Is QOS disabled for this particular export */
	if (export->bw_enabled || export->token_enabled) {
		(void)pepc_get_client(export, client_addr);
		if (!qos_check_pepc(export, size, op_type, caller_data, data,
				    QOS_PEPC)) {
			return 1;
		}
	}
	return 0;
}

/**
 * Function to process a QoS request.
 *
 * @param [in] size Size of the request.
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] data Pointer to the compound_data_t structure containing
 *			the request data.
 * @param [in] op_type Type of the operation (read/write).
 * @return 0 if successful, 1 otherwise.
 */
unsigned int QoS_Process(unsigned int size, void *caller_data,
			 compound_data_t *data, unsigned int op_type)
{
	unsigned int ret = 0;

	if (g_qos_config->qos_type == QOS_NOT_ENABLED ||
	    g_qos_config->enable_qos == 0) {
		ret = 0;
	} else if (g_qos_config->qos_type == QOS_PER_EXPORT_ENABLED) {
		ret = QoS_Process_pe(size, caller_data, data, op_type);
	} else if (g_qos_config->qos_type == QOS_PER_CLIENT_ENABLED) {
		ret = QoS_Process_pc(size, caller_data, data, op_type);
	} else if (g_qos_config->qos_type == QOS_PEREXPORT_PERCLIENT_ENABLED) {
		ret = QoS_Process_pepc(size, caller_data, data, op_type);
	} else {
		LogDebug(COMPONENT_QOS, " INVALID QOS_TYPE:%d",
			 g_qos_config->qos_type);
	}
	return ret;
}

/**
 * Function to get the QoS resume callback for the given operation type.
 *	Applicable for BW and token (Read/Write only)
 *
 * @param [in] op_type Type of the operation (read/write).
 * @return Pointer to the qos_svc_rcb function for the given operation type.
 */
qos_svc_rcb get_qos_resume_cb(unsigned int op_type)
{
	return (op_type == QOS_READ) ? nfs4_qos_read_cb : nfs4_qos_write_cb;
}

/**
 * Function to calculate the time until token refresh based on current time
 * and bucket information.
 *
 * @param [in] qos_class Pointer to the QoS client or export structure.
 * @param [in] class_type Type of the QoS class (export/client).
 * @param [in] op_type Type of the operation (read/write).
 * @param [in] ctime Current time in microseconds.
 * @return Time until token refresh in milliseconds.
 */
uint64_t qos_get_time_to_tokenrefresh(void *qos_class, unsigned int class_type,
				      unsigned int op_type, uint64_t ctime)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(qos_class, class_type, op_type);
	uint64_t ret =
		(((bucket->token_ldct + bucket->tokens_renew_time) - ctime) /
		 1000000);
	LogDebug(COMPONENT_QOS, "LTC:%ld TRT:%ld CT:%ld TO:%ld",
		 bucket->token_ldct, bucket->tokens_renew_time, ctime, ret);

	return ret;
}

/**
 * Function to allocate memory for qos_op_cb_arg structure and initialize it.
 *
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] ratecontrol Rate control flag.
 * @return Pointer to the newly allocated qos_op_cb_arg structure.
 */
static inline struct qos_op_cb_arg *alloc_qos_cb_args(void *caller_data,
						      int ratecontrol)
{
	struct qos_op_cb_arg *qos_cb_args = NULL;

	qos_cb_args = gsh_calloc(1, sizeof(struct qos_op_cb_arg));
	qos_cb_args->caller_data = caller_data;
	qos_cb_args->ratecontrol = ratecontrol;
	return qos_cb_args;
}

/**
 * Function to handle the token exhausted case and deffer the task.
 *
 * @param [in] ptr Pointer to the QoS client or export structure.
 * @param [in] caller_data Pointer to the caller data.
 * @param [in] data Pointer to compound_data_t containing the request data.
 * @param [in] class_type Type of the QoS class (export/client).
 * @param [in] op_type Type of the operation (read/write).
 */
static void qos_token_exausted_deffer_task(void *ptr, void *caller_data,
					   compound_data_t *data,
					   unsigned int class_type,
					   unsigned int op_type)
{
	qos_client_entry_t *client = NULL;
	timer_entry_t *new_timer_entry = NULL;
	unsigned int *num_ios_waiting = NULL;
	uint64_t timeout = 0;
	struct qos_op_cb_arg *qos_cb_args =
		alloc_qos_cb_args(caller_data, NON_RATELIMITING_IO);
	uint64_t ltime = get_time_in_usec();
	uint64_t time_to_refresh =
		qos_get_time_to_tokenrefresh(ptr, class_type, op_type, ltime);

	/* Considering 15 seconds delay for returning to client */
	timeout = get_time_future_useconds(
		ltime, MIN(THREAD_DELAY_NFS_ERR_DELAY_DEFAULT, time_to_refresh),
		0, 0);

	if (class_type == QOS_EXPORT) {
		client = get_and_insert_client_details(
			&(((qos_export_t *)ptr)->client_entries), data);
		num_ios_waiting = &(((qos_export_t *)ptr)->num_ios_waiting);
	} else {
		client = get_and_insert_client_details(
			&(((qos_client_t *)ptr)->client_entries), data);
		num_ios_waiting = &(((qos_client_t *)ptr)->num_ios_waiting);
	}

	if (client->num_ios_waiting >= 5 && client->epoll_disabled == 0) {
		client->epoll_disabled = 1;
		LogDebug(COMPONENT_QOS,
			 "Suspending Client Socket true :%p :%p ",
			 client->client_addr, client->rq_xprt);
		/*TODO: Need to uncomment once libntirpc changes
		 * gets in by Animesh Javali */
		/* svc_rqst_qos_suspend_socket(client->rq_xprt); */
	} else if (client->num_ios_waiting >= 5 &&
		   client->epoll_disabled == 1) {
		timeout = get_time_future_useconds(
			ltime, THREAD_DELAY_NFS_ERR_DELAY_IMMED, 0, 0);
	}

	new_timer_entry = create_timer_entry(
		timeout, get_qos_resume_cb(op_type), (void *)qos_cb_args);
	insert_timer_entry(&(client->io_waitlist_qos), new_timer_entry);
	client->num_ios_waiting++;
	(*num_ios_waiting)++;

	LogDebug(COMPONENT_QOS,
		 "new_timer %p io_w:%d c_io_w:%d CI:%p CT:%ld TO:%ld",
		 new_timer_entry, *num_ios_waiting, client->num_ios_waiting,
		 client->client_addr, ltime, timeout);
}

/**
 * Function to calculate the future time in microseconds.
 *
 * @param [in] current Current time in microseconds.
 * @param [in] seconds Number of seconds to add to the current time.
 * @param [in] mseconds Number of milliseconds to add to the current time.
 * @param [in] useconds Number of microseconds to add to the current time.
 * @return Future time in microseconds.
 */
static inline uint64_t get_time_future_useconds(uint64_t current,
						uint64_t seconds,
						uint64_t mseconds,
						uint64_t useconds)
{
	if (current == 0) {
		struct timespec ts;

		clock_gettime(CLOCK_MONOTONIC, &ts);
		/*  Convert to uSeconds */
		current = (ts.tv_sec * 1000000) + (ts.tv_nsec / 1000);
	}
	return (current + (seconds * 1000000) + (mseconds * 1000) + useconds);
}

/**
 * Function to deffer a task for bandwidth control
 *
 * @param [in] bucket Pointer to the bucket object
 * @param [in] caller_data Caller data passed to the callback function
 * @param [in] size Size of the data in bytes
 * @param [in] timeout Timeout for the task in microseconds
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void qos_bw_bucket_deffer_task(qos_bucket_t *bucket,
					     void *caller_data, uint64_t size,
					     uint64_t timeout,
					     unsigned int op_type)
{
	struct qos_op_cb_arg *qos_cb_args =
		alloc_qos_cb_args(caller_data, RATELIMITING_IO);
	timer_entry_t *new_timer_entry = create_timer_entry(
		timeout, get_qos_resume_cb(op_type), (void *)qos_cb_args);
	new_timer_entry->size = size;
	PTHREAD_MUTEX_lock(&bucket->lock);
	insert_timer_entry(&(bucket->io_waitlist_qos_bc), new_timer_entry);
	++bucket->num_ios_waiting;
	PTHREAD_MUTEX_unlock(&bucket->lock);
}

/**
 * Function to check bandwidth and delay based on current settings
 *	NON-Blocking IO
 * @param [in] bucket Pointer to the bucket object
 * @param [in] bytes Size of the data in bytes
 * @param [in] caller_data Caller data passed to the callback function
 * @param [in] op_type Type of the operation (read/write)
 * @return True if the bandwidth is sufficient, False otherwise
 */
static inline bool check_bandwidth_and_reschedule(qos_bucket_t *bucket,
						  uint64_t bytes,
						  void *caller_data,
						  unsigned int op_type)
{
	uint64_t last_time = bucket->bw_ldct;
	uint64_t current_time = get_time_in_usec();
	/* Microseconds required to meet bandwidth */
	uint64_t required_time = (bytes * 1000000) / bucket->max_bw_allowed;

	/* Condition will be true only after IDLE, or most of the time else */
	if (current_time < (last_time + required_time)) {
		/* Microseconds elapsed since last call */
		bucket->bw_ldct = last_time + required_time;

		qos_bw_bucket_deffer_task(bucket, caller_data, bytes,
					  last_time + required_time, op_type);
		return false;
	} else {
		if (current_time <
		    (last_time + required_time + BW_EXPORT_FW_IO_SCHEDULE))
			bucket->bw_ldct += required_time;
		else
			bucket->bw_ldct = current_time + required_time;

		return true;
	}
}

/**
 * Function to check bandwidth and delay based on current settings
 *	NON-Blocking IO
 * @param [in] bucket Pointer to the bucket object
 * @param [in] bytes Size of the data in bytes
 * @param [in] caller_data Caller data passed to the callback function
 * @param [in] op_type Type of the operation (read/write)
 * @return True if the bandwidth is sufficient, False otherwise
 */
static inline bool check_bandwidth_and_delay(qos_bucket_t *bucket,
					     uint64_t bytes, void *caller_data,
					     unsigned int op_type)
{
	uint64_t last_time = bucket->bw_ldct;
	uint64_t current_time = get_time_in_usec();
	int ret = 0;
	/* Microseconds elapsed since last call */
	uint64_t time_since_last_op = current_time - last_time;
	/* Microseconds required to meet bandwidth */
	uint64_t required_time = (bytes * 1000000) / bucket->max_bw_allowed;

	if (time_since_last_op < required_time) {
		/* Calculate delay in microseconds and sleep */
		uint64_t delay_time = required_time - time_since_last_op;
		struct timespec delay;

		delay.tv_sec = delay_time / 1000000;
		delay.tv_nsec = (delay_time % 1000000) * 1000;
		/* Enforce delay to limit bandwidth */
		ret = nanosleep(&delay, NULL);

		if (ret != 0)
			LogDebug(COMPONENT_QOS, "Sleep Failure ");
	}
	bucket->bw_ldct = get_time_in_usec();
	return true;
}

/**
 * Function to refresh tokens based on current settings
 *
 * @param [in] class_entry Pointer to the class entry object
 * @param [in] class_type Type of the QoS class (export/client)
 * @return True if tokens were refreshed, False otherwise
 */
static inline bool refresh_bucket_token(void *class_entry,
					unsigned int class_type,
					unsigned int op_type)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(class_entry, class_type, op_type);

	if (bucket == NULL)
		return 0;

	uint64_t ltime = get_time_in_usec();
	/* This is the logic for limiting the io based on  */
	if ((bucket->tokens_consumed >= bucket->max_available_tokens) &&
	    (ltime > (bucket->token_ldct + bucket->tokens_renew_time))) {
		LogDebug(COMPONENT_QOS, "CT:%ld LCT:%ld TRT:%ld cal:%ld", ltime,
			 bucket->token_ldct, bucket->tokens_renew_time,
			 (bucket->token_ldct + bucket->tokens_renew_time));
		bucket->tokens_consumed = 0;
		return 1;
	} else {
		return 0;
	}
}

/**
 * Function to refresh tokens for a QoS export
 *
 * @param [in] qos_class Pointer to the QoS export object
 */
static inline bool refresh_per_export_tokens(qos_export_t *export_entry)
{
	/* since the io waitlist queue are same for read and write ios,
	 * check for both and then return */
	return (refresh_bucket_token(export_entry, QOS_EXPORT, QOS_READ) ||
		refresh_bucket_token(export_entry, QOS_EXPORT, QOS_WRITE));
}

/**
 * Function to refresh tokens for a QoS client
 *
 * @param [in] qos_class Pointer to the QoS client object
 */
static inline bool refresh_per_client_tokens(qos_client_t *client_entry)
{
	/* since the io waitlist queue are same for read and write ios,
	 * check for both and then return */
	return (refresh_bucket_token(client_entry, QOS_CLIENT, QOS_READ) ||
		refresh_bucket_token(client_entry, QOS_CLIENT, QOS_WRITE));
}

/**
 * Function to create a new timer entry
 *
 * @param [in] expiry Expiry time of the timer in microseconds
 * @param [in] callback Callback function to be executed when the timer expires
 * @param [in] args Arguments to pass to the callback function
 * @return Pointer to the newly created timer entry
 */
static inline timer_entry_t *
create_timer_entry(uint64_t expiry, void (*callback)(void *), void *args)
{
	timer_entry_t *new_entry = gsh_calloc(1, sizeof(timer_entry_t));

	new_entry->expiry = expiry;
	new_entry->callback = callback;
	new_entry->args = args;
	LogDebug(COMPONENT_QOS, "Timer entry created:%p", new_entry);
	return new_entry;
}

/**
 * Function to allocate a client details structure for Token exhaust
 *
 * @param [in] data Compound data associated with the I/O request
 * @return Pointer to the newly allocated client details structure
 */
static qos_client_entry_t *alloc_clientdetails_pe(compound_data_t *data)
{
	qos_client_entry_t *new_entry = NULL;

	new_entry = gsh_calloc(1, sizeof(qos_client_entry_t));
	new_entry->client_addr = &op_ctx->client->cl_addrbuf;
	new_entry->data = data;
	new_entry->rq_xprt = data->req->rq_xprt;
	LogDebug(COMPONENT_QOS, "Adding Client entry CID:%p",
		 new_entry->client_addr);
	return new_entry;
}

/**
 * Function to get and insert a client details structure into
 * the token exhaust waitlist
 *
 * @param [in] head Pointer to the head of the waitlist
 * @param [in] data Compound data associated with the I/O request
 * @return Pointer to the client details structure
 */
static qos_client_entry_t *
get_and_insert_client_details(qos_client_entry_t **head, compound_data_t *data)
{
	qos_client_entry_t *new_entry = NULL;

	if (*head == NULL) {
		new_entry = alloc_clientdetails_pe(data);
		*head = new_entry;
		return new_entry;
	} else {
		qos_client_entry_t *current = *head;
		/*  used to insert at the end if client is not in list */
		qos_client_entry_t *temp = NULL;

		while (current != NULL &&
		       current->client_addr != &op_ctx->client->cl_addrbuf) {
			temp = current;
			current = current->next;
		}

		if (current == NULL) {
			new_entry = alloc_clientdetails_pe(data);
			temp->next = new_entry;
			return new_entry;
		} else {
			return current;
		}
	}
}

/**
 * Function to remove a client entry from the waitlist
 *
 * @param [in] head Pointer to the head of the waitlist
 * @param [in] entry_to_remove Pointer to the client entry to be removed
 */
static void remove_client_entry(qos_client_entry_t **head,
				qos_client_entry_t *entry_to_remove)
{
	LogDebug(COMPONENT_QOS,
		 "Removing Client entry head:%p remove: %p CID:%p ", *head,
		 entry_to_remove, entry_to_remove->client_addr);
	if (*head == NULL)
		return;

	if (*head == entry_to_remove) {
		*head = (*head)->next;
		gsh_free(entry_to_remove);
		return;
	}

	qos_client_entry_t *current = *head;

	while (current->next != NULL && current->next != entry_to_remove)
		current = current->next;

	if (current->next == entry_to_remove) {
		current->next = entry_to_remove->next;
		gsh_free(entry_to_remove);
	}
}

/**
 * Function to insert a timer entry into the waitlist
 *
 * @param [in] head Pointer to the head of the waitlist
 *	  head Pointer can be :
 *		1. Token Exhausted waitlist.
 *		2. BW control waitlist.
 *		3. IOPS control waitlist.
 * @param [in] new_entry Pointer to the new timer entry
 */
static void insert_timer_entry(timer_entry_t **head, timer_entry_t *new_entry)
{
	if (new_entry == NULL)
		LogDebug(COMPONENT_QOS, "ERROR new entry is NULL");

	if (*head == NULL || (*head)->expiry > new_entry->expiry) {
		new_entry->next = *head;
		*head = new_entry;
	} else {
		timer_entry_t *current = *head;

		while (current->next != NULL &&
		       current->next->expiry <= new_entry->expiry) {
			current = current->next;
		}
		new_entry->next = current->next;
		current->next = new_entry;
	}
}

/**
 * Function to remove a timer entry from the waitlist
 *
 * @param [in] head Pointer to the head of the waitlist
 * @param [in] entry_to_remove Pointer to the timer entry to be removed
 */
static void remove_timer_entry(timer_entry_t **head,
			       timer_entry_t *entry_to_remove)
{
	LogDebug(COMPONENT_QOS, "Timer entry head:%p remove: %p", *head,
		 entry_to_remove);
	if (*head == NULL)
		return;

	if (*head == entry_to_remove) {
		*head = (*head)->next;
		gsh_free(entry_to_remove);
		return;
	}

	timer_entry_t *current = *head;

	while (current->next != NULL && current->next != entry_to_remove)
		current = current->next;

	if (current->next == entry_to_remove) {
		current->next = entry_to_remove->next;
		gsh_free(entry_to_remove);
	}
}

/**
 * Function to release I/Os from the waitlist (Force release IO)
 *
 * @param [in] head Pointer to the head of the waitlist
 * @param [in] counter1 Pointer to a counter for the number of waiting I/Os.
 * @param [in] counter2 Pointer to counter for the number of waiting I/Os.
 *			will be used in case of IO got blocked on token exhaust
 */
static inline void release_wait_ios(timer_entry_t **head,
				    unsigned int *counter1,
				    unsigned int *counter2)
{
	timer_entry_t *current = *head;
	timer_entry_t *expired = NULL;

	while (current != NULL) {
		current->callback(current->args);
		LogDebug(COMPONENT_QOS,
			 "Force resume Timer:%p Expiry:%ld TCounter:%d",
			 current, current->expiry, *counter1);
		expired = current;
		current = current->next;
		remove_timer_entry(head, expired);
		--*counter1;
		--*counter2;
	}
}

/**
 * Function to execute expired timers in the waitlist
 *
 * @param [in] head Pointer to the head of the waitlist
 * @param [in] counter1 Pointer to a counter for the number of waiting I/Os.
 * @param [in] counter2 Pointer to another counter for the number of wait I/Os.
 */
static void execute_qos_expired_timers(timer_entry_t **head,
				       unsigned int *counter1,
				       unsigned int *counter2)
{
	uint64_t current_time = get_time_in_usec();
	timer_entry_t *current = *head;
	timer_entry_t *expired = NULL;

	while (current != NULL) {
		if (current->expiry <= current_time) {
			LogDebug(COMPONENT_QOS,
				 "Exp_IO_T:%p CT:%ld Ex:%ld Tco:%d Tco2:%d",
				 current, current_time, current->expiry,
				 *counter1, *counter2);
			current->callback(current->args);
			expired = current;
		}
		current = current->next;
		if (expired != NULL) {
			remove_timer_entry(head, expired);
			--*counter1;
			--*counter2;
			expired = NULL;
		}
	}
}

/**
 * Function to refresh client entries based on current token
 *
 * @param [in] clients Pointer to the head of the client entry list
 * @param [in] tokens_refreshed Flag indicating if tokens were refreshed
 * @param [in] qos_class Pointer to the QoS export or client object
 * @param [in] class_type Type of the QoS class (export/client)
 */
static inline void refresh_qos_client(qos_client_entry_t **clients,
				      bool tokens_refreshed, void *qos_class,
				      unsigned int class_type)
{
	qos_client_entry_t *client = *clients;
	unsigned int *counter1 = &(client->num_ios_waiting);
	unsigned int *counter2 =
		(class_type == QOS_EXPORT) ?
			&(((qos_export_t *)qos_class)->num_ios_waiting) :
			&(((qos_client_t *)qos_class)->num_ios_waiting);
	bool epd = 0;
	SVCXPRT *rq_xprt = NULL;

	LogDebug(COMPONENT_QOS, " CI:%p CWIO's:%d ", client->client_addr,
		 client->num_ios_waiting);
	if (tokens_refreshed) {
		release_wait_ios(&(client->io_waitlist_qos), counter1,
				 counter2);
	} else if (client->num_ios_waiting) {
		execute_qos_expired_timers(&(client->io_waitlist_qos), counter1,
					   counter2);
	}

	if (client->num_ios_waiting == 0) {
		rq_xprt = client->rq_xprt;
		epd = client->epoll_disabled;
		LogDebug(COMPONENT_QOS,
			 "Resuming Client Socket Cid:%p Xprt:%p epd:%d xprt:%p",
			 client->client_addr, client->rq_xprt,
			 client->epoll_disabled, rq_xprt);
		remove_client_entry(clients, client);
		if (epd == 1) {
			/* TODO: Need to uncomment once libntirpc
			 * changes gets in by Animesh Javali */
			/* svc_rqst_qos_resume_socket(rq_xprt); */
			epd = 0;
		}
		rq_xprt = NULL;
	}
}

/**
 * Function to refresh tokens export/client
 *
 * @param [in] class Pointer to the QoS export object
 * @param [in] qos_class_type Type indicating QoS export or Client class
 */
static inline void refresh_qos_token_by_class(void *class,
					      unsigned int qos_class_type)
{
	bool tokens_refreshed = 0;
	qos_client_entry_t *client = NULL;
	qos_client_entry_t *temp = NULL;

	if (qos_class_type == QOS_EXPORT) {
		qos_export_t *qos_class = class;

		tokens_refreshed = refresh_per_export_tokens(qos_class);
		LogDebug(COMPONENT_QOS, " SN:%d TR:%d WIO's:%d",
			 qos_class->export_id, tokens_refreshed,
			 qos_class->num_ios_waiting);
		PTHREAD_MUTEX_lock(&(qos_class->lock));
		client = qos_class->client_entries;
		while (client != NULL) {
			temp = client->next;
			refresh_qos_client(&(qos_class->client_entries),
					   tokens_refreshed, qos_class,
					   QOS_EXPORT);
			client = temp;
		}
		PTHREAD_MUTEX_unlock(&(qos_class->lock));
	} else {
		qos_client_t *qos_class = class;

		tokens_refreshed = refresh_per_client_tokens(qos_class);
		LogDebug(COMPONENT_QOS, " CI:%p TR:%d WIO's:%d",
			 qos_class->client_addr, tokens_refreshed,
			 qos_class->num_ios_waiting);
		PTHREAD_MUTEX_lock(&(qos_class->lock));
		client = qos_class->client_entries;
		while (client != NULL) {
			temp = client->next;
			refresh_qos_client(&(qos_class->client_entries),
					   tokens_refreshed, qos_class,
					   QOS_CLIENT);
			client = temp;
		}
		PTHREAD_MUTEX_unlock(&(qos_class->lock));
	}
}

/**
 * Callback function to control tokens in pepc conf.
 *
 * @param [in] export Pointer to gsh_export structure representing the export.
 * @param [in] state Pointer to the state data (not used).
 * @return True to continue iteration.
 */
bool pepc_token_control_cb(struct gsh_export *gsh_export, void *state)
{
	qos_export_t *export = get_export_qos(gsh_export);

	if (export && export->token_enabled) {
		LogDebug(COMPONENT_QOS, "going for token refresh:%s",
			 gsh_export->cfg_fullpath);
		refresh_qos_token_by_class(export, QOS_EXPORT);
		/* In a export check any client exausted the token
		 * and is replinish value reached */
		qos_client_t *client = export->clients;

		while (client != NULL) {
			refresh_qos_token_by_class(client, QOS_CLIENT);
			client = client->next;
		}
	}
	/* Continue iteration */
	return true;
}

/**
 * Callback function to control tokens in PS conf.
 *
 * @param [in] export Pointer to gsh_export structure representing the export.
 * @param [in] state Pointer to the state data (not used).
 * @return True if the iteration should continue, False otherwise.
 */
bool ps_token_control_cb(struct gsh_export *gsh_export, void *state)
{
	qos_export_t *export = get_export_qos(gsh_export);

	if (export && export->token_enabled) {
		LogDebug(COMPONENT_QOS, "going for token refresh:%s",
			 gsh_export->cfg_fullpath);
		refresh_qos_token_by_class(export, QOS_EXPORT);
	}
	/* Continue iteration */
	return true;
}

/**
 * Callback function to control tokens in PC conf.
 *
 * @param [in] cl Pointer to gsh_client structure representing the client.
 * @param [in] state Pointer to the state data (not used).
 * @return True if the iteration should continue, False otherwise.
 */
bool pc_token_control_cb(struct gsh_client *cl, void *state)
{
	qos_client_t *client = get_client_qos(cl);

	if (client && client->token_enabled) {
		LogDebug(COMPONENT_QOS, "going for token refresh:%p",
			 &cl->cl_addrbuf);
		refresh_qos_token_by_class(client, QOS_CLIENT);
	}
	/* Continue iteration */
	return true;
}

/**
 * Function to refresh tokens based on the QoS configuration.
 *
 * @return None
 */
static inline void refresh_qos_token(void)
{
	/*  Token refershing function calls here
	 *  export based refresing
	 *  Client based refresing
	 *  Perexport-PerClient based refresing
	 *  Group Based
	 *  Directory level */
	int op_type = 0;

	switch (g_qos_config->qos_type) {
	case QOS_NOT_ENABLED:
		LogDebug(COMPONENT_QOS, "QOS not enabled :%d",
			 g_qos_config->qos_type);
		break;
	case QOS_PER_EXPORT_ENABLED:
		foreach_gsh_export(ps_token_control_cb, false, &op_type);
		break;
	case QOS_PER_CLIENT_ENABLED:
		foreach_gsh_client(pc_token_control_cb, &op_type);
		break;
	case QOS_PEREXPORT_PERCLIENT_ENABLED:
		foreach_gsh_export(pepc_token_control_cb, false, &op_type);
		break;
	default:
		LogDebug(COMPONENT_QOS, " Something really wrong:%d",
			 g_qos_config->qos_type);
		break;
	}
}

/**
 * Function to resume bandwidth I/Os form a bucket
 * resumes the expired IO from the bucket.
 * @param [in] bucket Pointer to the bucket object
 */
static inline void resume_bw_bucket_io(qos_bucket_t *bucket)
{
	uint32_t dummy_counter = UINT32_MAX;

	execute_qos_expired_timers(&(bucket->io_waitlist_qos_bc),
				   &(bucket->num_ios_waiting), &dummy_counter);
}

/**
 * Function to resume bandwidth I/Os for a export level QoS configuration
 *
 * @param [in] export Pointer to the QoS export object
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void resume_bw_io_pe(qos_export_t *export, unsigned int op_type)
{
	if (export != NULL) {
		qos_bucket_t *bucket =
			qos_get_bw_bucket(export, QOS_EXPORT, op_type);

		if (bucket == NULL)
			return;

		PTHREAD_MUTEX_lock(&bucket->lock);
		resume_bw_bucket_io(bucket);
		PTHREAD_MUTEX_unlock(&bucket->lock);
	}
}

/**
 * Function to resume bandwidth I/Os for a client level QoS configuration
 *
 * @param [in] client Pointer to the QoS client object
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void resume_bw_io_pc(qos_client_t *client, unsigned int op_type)
{
	if (client != NULL) {
		qos_bucket_t *bucket =
			qos_get_bw_bucket(client, QOS_CLIENT, op_type);

		if (bucket == NULL)
			return;

		PTHREAD_MUTEX_lock(&bucket->lock);
		resume_bw_bucket_io(bucket);
		PTHREAD_MUTEX_unlock(&bucket->lock);
	}
}

/**
 * Function to resume bandwidth I/Os for a pepc QoS configuration
 *
 * @param [in] export Pointer to the QoS export object
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void resume_bw_io_pepc(qos_export_t *export, unsigned int op_type)
{
	if (export == NULL)
		return;

	uint64_t current_time = get_time_in_usec();
	int check_delay = ((op_type == QOS_READ) ? BW_EXPORT_FW_IO_SCHEDULE :
						   BW_DELAY_USEC);
	qos_bucket_t *bucket = qos_get_bw_bucket(export, QOS_PEPC, op_type);

	if (bucket == NULL)
		return;

	PTHREAD_MUTEX_lock(&(bucket->lock));
	timer_entry_t *io_entry = bucket->io_waitlist_qos_bc;

	/*  Since IO load is not there,
	 *  its possible again we entered here immediately */
	while ((io_entry != NULL) &&
	       (bucket->bw_ldct < (current_time + check_delay))) {
		uint64_t required_time_for_io =
			(io_entry->size * 1000000) / bucket->max_bw_allowed;

		/* Under heavy IO load, and multiple exports, consider enough
		 * time looking backward for acutal BW calculation
		 * and consumption
		 **/
		if (((bucket->bw_ldct + required_time_for_io +
		      BW_EXPORT_FW_IO_SCHEDULE) > current_time)) {
			/* Resuming BW from last IO completion */
			bucket->bw_ldct =
				bucket->bw_ldct + required_time_for_io;
		} else {
			/* Resuming BW from IDLE */
			bucket->bw_ldct = current_time;
		}

		--bucket->num_ios_waiting;
		bucket->io_waitlist_qos_bc = io_entry->next;
		io_entry->callback(io_entry->args);
		LogDebug(COMPONENT_QOS, "Timer entry resumed:%p", io_entry);
		gsh_free(io_entry);
		io_entry = bucket->io_waitlist_qos_bc;
	}
	PTHREAD_MUTEX_unlock(&(bucket->lock));
}

/**
 * Function to reschedule I/O operations from client bucket to export bucket.
 *
 * @param [in] sbucket Pointer to qos_bucket_t representing the export bucket.
 * @param [in] cbucket Pointer to qos_bucket_t representing the client bucket.
 * @param [in] current_time Current time in microseconds.
 */
static inline void pepc_rescedule_io_to_export(qos_bucket_t *sbucket,
					       qos_bucket_t *cbucket,
					       uint64_t current_time)
{
	uint64_t clienttime = current_time;
	timer_entry_t *io_entry = NULL;
	uint64_t required_time_for_io = 0;

pick_next_io:
	io_entry = cbucket->io_waitlist_qos_bc;

	if (io_entry == NULL)
		return;

	required_time_for_io =
		(io_entry->size * 1000000) / cbucket->max_bw_allowed;

	/* This check ensures we dont exceed the Client bucket Limit */
	if (clienttime + BW_DELAY_USEC >= cbucket->bw_ldct) {
		/* below if ensures full BW is available for this client
		 * else indicate export limit has been reached
		 * so client is trottling */
		if ((cbucket->bw_ldct + required_time_for_io +
		     BW_EXPORT_FW_IO_SCHEDULE) > current_time) {
			cbucket->bw_ldct =
				cbucket->bw_ldct + required_time_for_io;
			io_entry->expiry = cbucket->bw_ldct;
		} else {
			cbucket->bw_ldct = current_time;
			io_entry->expiry = current_time;
		}
		cbucket->io_waitlist_qos_bc = io_entry->next;
		io_entry->next = NULL;
		insert_timer_entry(&(sbucket->io_waitlist_qos_bc), io_entry);
		++sbucket->num_ios_waiting;
		--cbucket->num_ios_waiting;

		/*  Check ensures scheduling future IO till
		 *  (current_time + BW_CLIENT_FW_IO_SCHEDULE) time */
		if (cbucket->bw_ldct <
		    (current_time + BW_CLIENT_FW_IO_SCHEDULE)) {
			clienttime = cbucket->bw_ldct;
			goto pick_next_io;
		}
	}
}

/**
 * Function to reschedule bandwidth I/Os for a export
 *
 * @param [in] export Pointer to the QoS export object
 * @param [in] op_type Type of the operation (read/write).
 */
static inline void pepc_reschedule_bw_io(qos_export_t *export,
					 unsigned int op_type)
{
	if (export == NULL)
		return;

	uint64_t current_time = get_time_in_usec();
	qos_client_t *client = export->clients;
	qos_bucket_t *sbucket = qos_get_bw_bucket(export, QOS_PEPC, op_type);

	if (sbucket == NULL)
		return;
	if (current_time > sbucket->bw_ldct) {
		PTHREAD_MUTEX_lock(&sbucket->lock);
		while (client != NULL) {
			qos_bucket_t *cbucket =
				qos_get_bw_bucket(client, QOS_CLIENT, op_type);

			if (cbucket != NULL &&
			    current_time >= cbucket->bw_ldct &&
			    cbucket->io_waitlist_qos_bc != NULL) {
				PTHREAD_MUTEX_lock(&cbucket->lock);
				pepc_rescedule_io_to_export(sbucket, cbucket,
							    current_time);
				PTHREAD_MUTEX_unlock(&cbucket->lock);
			}

			client = client->next;
		}
		PTHREAD_MUTEX_unlock(&sbucket->lock);
	}
}

/**
 * @brief Callback function for IO control of QoS export.
 *	Function responsible to initate the IO resume for export.
 *
 * @param [in] export Pointer to gsh_export structure.
 * @param [in] state Void pointer containing op_type.
 *
 * @return true to continue iteration
 */
bool ps_io_control_cb(struct gsh_export *gsh_export, void *state)
{
	qos_export_t *export = get_export_qos(gsh_export);

	if (export && export->bw_enabled)
		resume_bw_io_pe(export, *(unsigned int *)state);

	if (export && export->iops_enabled)
		resume_iops_pe(export, *(unsigned int *)state);

	/* Continue iteration */
	return true;
}
/**
 * @brief Callback function for IO control of Qos Client.
 *	Function responsible to initate the IO resume for client
 *
 * @param [in] cl Pointer to gsh_client structure.
 * @param [in] state Void pointer containing op_type.
 *
 * @return true to continue iteration.
 */
bool pc_io_control_cb(struct gsh_client *cl, void *state)
{
	qos_client_t *client = get_client_qos(cl);

	if (client && client->bw_enabled)
		resume_bw_io_pc(client, *(unsigned int *)state);

	if (client && client->iops_enabled)
		resume_iops_pc(client, *(unsigned int *)state);

	/* Continue iteration */
	return true;
}
/**
 * @brief Callback function for IO control of pepc.
 *	Function responsible to initate the IO resume at export
 *	and rescheduling IO from Client bucket to export bucket.
 *
 * @param [in] export Pointer to gsh_export structure representing the export
 * @param [in] state Void pointer containing op_type.
 *
 * @return true to continue iteration.
 */
bool pepc_io_control_cb(struct gsh_export *gsh_export, void *state)
{
	qos_export_t *export = get_export_qos(gsh_export);

	if (export && export->bw_enabled) {
		pepc_reschedule_bw_io(export, *(unsigned int *)state);
		resume_bw_io_pepc(export, *(unsigned int *)state);
	}

	if (export && export->iops_enabled) {
		pepc_reschedule_iops(export, *(unsigned int *)state);
		resume_iops_pepc(export, *(unsigned int *)state);
	}
	/* Continue iteration */
	return true;
}

/**
 * Function to resume bandwidth ios and OPS ios based on operation type
 *
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void resume_io(unsigned int op_type)
{
	switch (g_qos_config->qos_type) {
	case QOS_NOT_ENABLED:
		LogDebug(COMPONENT_QOS, "QOS not enabled :%d",
			 g_qos_config->qos_type);
		break;
	case QOS_PER_EXPORT_ENABLED:
		foreach_gsh_export(ps_io_control_cb, false, &op_type);
		break;
	case QOS_PER_CLIENT_ENABLED:
		foreach_gsh_client(pc_io_control_cb, &op_type);
		break;
	case QOS_PEREXPORT_PERCLIENT_ENABLED:
		foreach_gsh_export(pepc_io_control_cb, false, &op_type);
		break;
	default:
		LogDebug(COMPONENT_QOS, " Something really wrong:%d",
			 g_qos_config->qos_type);
		break;
	}
}

/**
 * @brief Main function for QoS thread.
 *
 * This function is the entry point for each QoS worker thread.
 * It continuously runs, 2 threads are there which takes care of,
 * resuming bandwidth I/O operations,
 * resuming IOPS operations,
 * and handling token refresh tasks if enabled in the configuration.
 *
 * @param [in] arg Pointer to details of op_type (QOS_READ or QOS_WRITE).
 *
 * @return NULL on successful completion
 */
static void *qos_thread_func(void *arg)
{
	pthread_t current_thread_id = pthread_self();
	int counter = 0;
	unsigned int op_type = *(unsigned int *)arg;

	LogDebug(COMPONENT_QOS, "running from arg:%d thread.id:%lu ", op_type,
		 current_thread_id);
	while (true) {
		resume_io(op_type);
		/* Currently combined accounting is enabled only in writebucket,
		 * once pnfs and nconnect gets properly enabled
		 * need to revisit this condition: "op_type == QOS_WRITE"
		 */
		if (g_qos_config->enable_tokens &&
		    counter >= TOKEN_REFRESH_DELAY && op_type == QOS_WRITE) {
			LogDebug(COMPONENT_QOS,
				 "Periodic task in qos worker thread.id:%lu ",
				 current_thread_id);
			refresh_qos_token();
			counter = 0;
		}
		/* Periodic Wakeup */
		usleep(BW_DELAY_USEC / 2);
		counter++;
	}
	return NULL;
}

pthread_t qos_thread[2] = { 0, 0 };
int var[2] = { QOS_READ, QOS_WRITE };

/**
 * @brief Initialize the QoS threads.
 *
 * This function initializes the two QoS worker threads,
 * if QoS is enabled in the configuration.
 *
 * @note The threads are detached after creation.
 */
static void qos_thread_init(void)
{
	if (g_qos_config->enable_qos == 0)
		return;

	PTHREAD_MUTEX_lock(&g_qos_lock);
	if (qos_initalized == 0) {
		qos_initalized = 1;
		for (int i = 0; i < 2; i++) {
			pthread_create(&qos_thread[i], NULL, qos_thread_func,
				       &var[i]);
			LogDebug(COMPONENT_QOS, "Qos thread created :%lu",
				 qos_thread[i]);
			pthread_detach(qos_thread[i]);
		}
	}
	PTHREAD_MUTEX_unlock(&g_qos_lock);
}

/**
 * @brief Get the QoS client class associated with a client.
 * This function retrieves QoS client class for a given gsh_client structure.
 *
 * @param [in] client Pointer to the gsh_client structure
 *
 * @return qos_client_t pointer to QoS client class, or NULL if not found
 */
qos_client_t *get_client_qos(struct gsh_client *gsh_client)
{
	qos_client_t *qos_class = NULL;

	if (gsh_client && gsh_client->qos_class != NULL)
		qos_class = gsh_client->qos_class;

	return qos_class;
}

/**
 * @brief Get the QoS export class associated with an export.
 * function retrieves the QoS export class for a given gsh_export structure.
 *
 * @param [in] gsh_export Pointer to the gsh_export structure
 *
 * @return qos_export_t pointer to QoS export class, or NULL if not found
 */
qos_export_t *get_export_qos(struct gsh_export *gsh_export)
{
	qos_export_t *qos_class = NULL;

	if (gsh_export && gsh_export->qos_class != NULL)
		qos_class = gsh_export->qos_class;

	return qos_class;
}

/**
 * @brief Get the number of clients associated with a QoS export.
 *
 * This function counts the number of clients in a given QoS export class.
 * Used in case of reply to User via DBUS
 *
 * @param [in] e_qos_class Pointer to qos_export_t representing the QoS export
 *
 * @return uint32_t number of clients in the export, 0 if the export is NULL
 */
uint32_t get_export_client_count(qos_export_t *e_qos_class)
{
	uint32_t count = 0;

	if (e_qos_class == NULL)
		return count;

	qos_client_t *c_qos_class = e_qos_class->clients;

	while (c_qos_class) {
		count++;
		c_qos_class = c_qos_class->next;
	}

	return count;
}

/**
 * @brief Defer IOPS task for a bucket.
 *
 * This function defers an IOPS task by adding it to the bucket's I/O wait list
 * and setting a timer to handle the task after a specified timeout.
 * It allocates memory for the QoS operation callback arguments,
 * creates a new timer entry
 * and inserts it into the appropriate waitlist.
 *
 * @param [in] bucket Pointer to the qos_bucket_t representing the bucket
 * @param [in] caller_data Caller-specific data to be passed with the IOPS task
 * @param [in] size Size of the IOPS task
 * @param [in] timeout in microseconds after which IOPS task should be executed.
 */
void qos_iops_deffer_task(qos_bucket_t *bucket, void *caller_data,
			  uint64_t size, uint64_t timeout)
{
	struct qos_op_cb_arg *qos_cb_args =
		alloc_qos_cb_args(caller_data, RATELIMITING_IO);
	timer_entry_t *new_timer_entry = create_timer_entry(
		timeout, nfs4_qos_compond_cb, (void *)qos_cb_args);
	new_timer_entry->size = size;
	PTHREAD_MUTEX_lock(&bucket->lock);
	bucket->iops_consumed += size;
	insert_timer_entry(&(bucket->io_waitlist_qos_iops), new_timer_entry);
	++bucket->num_ios_waiting;
	PTHREAD_MUTEX_unlock(&bucket->lock);
}

/**
 * Function to check and take decision for IOPS.
 *
 * @param [in] data Compound data associated with the I/O request
 *
 * @return True if the IOPS is processed asynchronously, False otherwise
 */
int qos_iops_check(compound_data_t *data, qos_bucket_t *bucket)
{
	uint64_t last_time = bucket->iops_ldct;
	uint64_t current_time = get_time_in_usec();
	uint64_t timeout = 0;
	/*max compound can be 100 only*/
	uint8_t num_ops = data->argarray_len;
	uint64_t required_time_for_io =
		(num_ops * (USEC_IN_SEC / bucket->max_iops_allowed));

	/*Set the compound ops accounted bit*/
	data->qos_flags |= IS_QOS_IOPS_ACCOUNTED;

	/* Accounting for the previous 5 Milliseconds also,
	 * so that algo doesn't missed the limit */
	if (current_time <
	    (last_time + IOPS_DELAY_USEC + required_time_for_io)) {
		bucket->iops_ldct = bucket->iops_ldct + required_time_for_io;
	} else {
		bucket->iops_ldct = current_time + required_time_for_io;
	}
	timeout = bucket->iops_ldct;

	/* The whole compound has been Accounted for */
	if (timeout <= current_time) {
		bucket->iops_consumed += num_ops;
		return QOS_TASK_ASYNC_NOT_SCHEDULED;
	} else {
		qos_iops_deffer_task(bucket, data, num_ops, timeout);
		return QOS_TASK_ASYNC_SCHEDULED;
	}
}

/**
 * Function to process IOPS for a export level QoS configuration
 *
 * @param [in] data Compound data associated with the I/O request
 *
 * @return True if the IOPS is processed asynchronously, False otherwise
 */
int QoS_Process_iops_pe(compound_data_t *data, uint32_t op_type)
{
	qos_export_t *export = op_ctx->ctx_export->qos_class;
	qos_bucket_t *bucket = NULL;
	int ret = 0;

	LogFullDebug(COMPONENT_QOS, "export:%s ",
		     op_ctx->ctx_export->cfg_fullpath);
	if (export == NULL) {
		PTHREAD_MUTEX_lock(&g_qos_lock);
		if (op_ctx->ctx_export->qos_class == NULL)
			QoS_perExportInsert(op_ctx->ctx_export, g_qos_config);

		export = op_ctx->ctx_export->qos_class;
		PTHREAD_MUTEX_unlock(&g_qos_lock);
	}

	if (export->iops_enabled == false)
		return QOS_TASK_ASYNC_NOT_SCHEDULED;

	bucket = qos_get_iops_bucket(export, QOS_EXPORT, op_type);
	if (bucket == NULL) {
		ret = QOS_TASK_ASYNC_NOT_SCHEDULED;
		goto out;
	}

	PTHREAD_MUTEX_lock(&export->lock);
	ret = qos_iops_check(data, bucket);
	PTHREAD_MUTEX_unlock(&export->lock);
out:
	return ret;
}

/**
 * Function to process IOPS for a client level QoS configuration
 *
 * @param [in] data Compound data associated with the I/O request.
 *
 * @return True if the IOPS is processed asynchronously, False otherwise
 */
int QoS_Process_iops_pc(compound_data_t *data, uint32_t op_type)
{
	qos_client_t *client = op_ctx->client->qos_class;
	qos_bucket_t *bucket = NULL;
	int ret = 0;

	LogFullDebug(COMPONENT_QOS, "client:%p ",
		     &(op_ctx->client->cl_addrbuf));
	if (client == NULL) {
		PTHREAD_MUTEX_lock(&g_qos_lock);
		/* Since this is QOS_PC, pass the global QOS values */
		if (op_ctx->client->qos_class == NULL)
			QoS_perClientInsert(g_qos_config, op_ctx->client);

		client = op_ctx->client->qos_class;
		PTHREAD_MUTEX_unlock(&g_qos_lock);
	}

	if (client->iops_enabled == false)
		return QOS_TASK_ASYNC_NOT_SCHEDULED;

	bucket = qos_get_iops_bucket(client, QOS_CLIENT, op_type);
	if (bucket == NULL) {
		ret = QOS_TASK_ASYNC_NOT_SCHEDULED;
		goto out;
	}
	PTHREAD_MUTEX_lock(&client->lock);
	ret = qos_iops_check(data, bucket);
	PTHREAD_MUTEX_unlock(&client->lock);
out:
	return ret;
}

/**
 * Function to process IOPS for pepc QoS configuration
 *
 * @param [in] data Compound data associated with the I/O request
 *
 * @return True if the IOPS is processed asynchronously, False otherwise
 */
int QoS_Process_iops_pepc(compound_data_t *data, uint32_t op_type)
{
	qos_export_t *export = op_ctx->ctx_export->qos_class;
	sockaddr_t *client_addr = &op_ctx->client->cl_addrbuf;

	LogFullDebug(COMPONENT_QOS, "export:%s ",
		     op_ctx->ctx_export->cfg_fullpath);
	if (export == NULL) {
		/*  Execution reached here means QOS is enabled but,
		 *  QOS block is not populated for this export
		 *  so apply the global values to the export values
		 *  and mark the qos_enabled to false, we need this in case of
		 *  runtime enablement of QOS*/
		PTHREAD_MUTEX_lock(&g_qos_lock);
		/* On runtime enablement of QOS, or due to only global config
		 * present, there is possiblity of multiple IO's rushing
		 * into this function for this paticular export,
		 * so recheck before going for allocation*/
		export = op_ctx->ctx_export->qos_class;
		if (export == NULL) {
			QoS_perExportInsert(op_ctx->ctx_export, g_qos_config);
			export = op_ctx->ctx_export->qos_class;
		}
		PTHREAD_MUTEX_unlock(&g_qos_lock);
	}

	/* Is QOS iops disabled for this particular export */
	if (export->iops_enabled) {
		qos_client_t *client = NULL;
		qos_bucket_t *bucket = NULL;

		client = pepc_get_client(export, client_addr);
		bucket = qos_get_iops_bucket(client, QOS_CLIENT, op_type);

		if (bucket == NULL)
			goto out;

		data->qos_flags |= IS_QOS_IOPS_ACCOUNTED;
		qos_iops_deffer_task(bucket, data, data->argarray_len,
				     get_time_in_usec());

		return QOS_TASK_ASYNC_SCHEDULED;
	}

out:
	return QOS_TASK_ASYNC_NOT_SCHEDULED;
}

/**
 * Function to process IOPS (Hook)
 *
 * @param [in] data Compound data associated with the I/O request
 * @return True if the IOPS is processed asynchronously, False otherwise
 */
unsigned int QoS_Process_iops(compound_data_t *data)
{
	unsigned int ret = QOS_TASK_ASYNC_NOT_SCHEDULED;
	uint32_t op_type = QOS_WRITE;

	if ((g_qos_config->enable_qos == 0) ||
	    (g_qos_config->enable_iops_control == 0) ||
	    (op_ctx->ctx_export == NULL) ||
	    (strlen(op_ctx->ctx_export->cfg_fullpath) <= 2))
		return ret;

	if (g_qos_config->qos_type == QOS_PER_EXPORT_ENABLED)
		ret = QoS_Process_iops_pe(data, op_type);
	else if (g_qos_config->qos_type == QOS_PER_CLIENT_ENABLED)
		ret = QoS_Process_iops_pc(data, op_type);
	else if (g_qos_config->qos_type == QOS_PEREXPORT_PERCLIENT_ENABLED)
		ret = QoS_Process_iops_pepc(data, op_type);

	LogFullDebug(COMPONENT_QOS,
		     "oppos:%d opcode:%d qos_flags:%d isaccouted:%d ret:%d",
		     data->oppos, data->opcode, data->qos_flags,
		     ((data->qos_flags & IS_QOS_IOPS_ACCOUNTED) ? 1 : 0), ret);
	return ret;
}

/**
 * Function to resume expired IOPS form bucket
 *
 * @param [in] bucket Pointer to the bucket object
 */
static inline void resume_iops_bucket(qos_bucket_t *bucket)
{
	uint32_t dummy_counter = UINT32_MAX;

	execute_qos_expired_timers(&(bucket->io_waitlist_qos_iops),
				   &(bucket->num_ios_waiting), &dummy_counter);
}

/**
 * Function to resume IOPS io in Perexport Conf
 *
 * @param [in] export Pointer to the QoS export object
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void resume_iops_pe(qos_export_t *export, unsigned int op_type)
{
	if (export != NULL) {
		qos_bucket_t *bucket =
			qos_get_iops_bucket(export, QOS_EXPORT, op_type);

		if (bucket == NULL)
			return;

		PTHREAD_MUTEX_lock(&bucket->lock);
		resume_iops_bucket(bucket);
		PTHREAD_MUTEX_unlock(&bucket->lock);
	}
}

/**
 * Function to resume IOPS io in PerClient Conf
 *
 * @param [in] export Pointer to the QoS client object
 * @param [in] op_type Type of the operation (read/write)
 */
static inline void resume_iops_pc(qos_client_t *client, unsigned int op_type)
{
	if (client != NULL) {
		qos_bucket_t *bucket =
			qos_get_iops_bucket(client, QOS_CLIENT, op_type);

		if (bucket == NULL)
			return;

		PTHREAD_MUTEX_lock(&bucket->lock);
		resume_iops_bucket(bucket);
		PTHREAD_MUTEX_unlock(&bucket->lock);
	}
}

/**
 * Function to reschedule IOPS from a client bucket to the export bucket
 *
 * @param [in] sbucket Pointer to the export bucket object
 * @param [in] cbucket Pointer to the client bucket object
 * @param [in] current_time Current time in microseconds
 */
static inline void pepc_rescedule_iops_to_export(qos_bucket_t *sbucket,
						 qos_bucket_t *cbucket,
						 uint64_t current_time)
{
	uint64_t clienttime = current_time;
	timer_entry_t *io_entry = NULL;
	uint64_t required_time_for_io = 0;

pick_next_io:
	io_entry = cbucket->io_waitlist_qos_iops;

	if (io_entry == NULL)
		return;

	required_time_for_io =
		(io_entry->size * (USEC_IN_SEC / cbucket->max_iops_allowed));

	/* This check ensures we dont exceed the Client bucket Limit */
	if (clienttime + IOPS_DELAY_USEC >= cbucket->iops_ldct) {
		/* below if ensures full IOPS is available for this client
		 * else indicate export limit has been reached
		 * so client is trottling */
		if ((cbucket->iops_ldct + required_time_for_io +
		     IOPS_EXPORT_FW_IO_SCHEDULE) > current_time) {
			cbucket->iops_ldct =
				cbucket->iops_ldct + required_time_for_io;
			io_entry->expiry = cbucket->iops_ldct;
		} else {
			cbucket->iops_ldct = current_time;
			io_entry->expiry = current_time;
		}
		cbucket->io_waitlist_qos_iops = io_entry->next;
		io_entry->next = NULL;
		insert_timer_entry(&(sbucket->io_waitlist_qos_iops), io_entry);
		++sbucket->num_ios_waiting;
		--cbucket->num_ios_waiting;

		/*  Check ensures scheduling future IO till
		 *  (current_time + IOPS_CLIENT_FW_IO_SCHEDULE) time */
		if (cbucket->iops_ldct <
		    (current_time + IOPS_CLIENT_FW_IO_SCHEDULE)) {
			clienttime = cbucket->iops_ldct;
			goto pick_next_io;
		}
	}
}

/**
 * Function to reschedule IOPS io from client bucket to export bucket
 * Checks for client bucket limit.
 *
 * @param [in] export Pointer to the QoS export object
 * @param [in] op_type Type of the operation (read/write).
 */
static inline void pepc_reschedule_iops(qos_export_t *export,
					unsigned int op_type)
{
	if (export == NULL)
		return;

	uint64_t current_time = get_time_in_usec();
	qos_client_t *client = export->clients;
	qos_bucket_t *sbucket = qos_get_iops_bucket(export, QOS_PEPC, op_type);

	if (sbucket == NULL)
		return;

	if (current_time > sbucket->iops_ldct) {
		PTHREAD_MUTEX_lock(&sbucket->lock);
		while (client != NULL) {
			qos_bucket_t *cbucket = qos_get_iops_bucket(
				client, QOS_CLIENT, op_type);

			if (cbucket != NULL &&
			    current_time >= cbucket->iops_ldct &&
			    cbucket->io_waitlist_qos_iops != NULL) {
				PTHREAD_MUTEX_lock(&cbucket->lock);
				pepc_rescedule_iops_to_export(sbucket, cbucket,
							      current_time);
				PTHREAD_MUTEX_unlock(&cbucket->lock);
			}

			client = client->next;
		}
		PTHREAD_MUTEX_unlock(&sbucket->lock);
	}
}

/**
 * Function to resume the IOPS waiting in export based on limit set in conf
 *
 * @param [in] export Pointer to the QoS export object
 * @param [in] op_type Type of the operation (read/write).
 */
static inline void resume_iops_pepc(qos_export_t *export, unsigned int op_type)
{
	if (export == NULL)
		return;

	uint64_t current_time = get_time_in_usec();
	int check_delay = ((op_type == QOS_READ) ? IOPS_EXPORT_FW_IO_SCHEDULE :
						   IOPS_DELAY_USEC);
	qos_bucket_t *bucket = qos_get_iops_bucket(export, QOS_PEPC, op_type);

	if (bucket == NULL)
		return;

	PTHREAD_MUTEX_lock(&(bucket->lock));
	timer_entry_t *io_entry = bucket->io_waitlist_qos_iops;

	/*  In less load situtation,
	 *  its possible again we entered here immediately */
	while ((io_entry != NULL) &&
	       (bucket->iops_ldct < (current_time + check_delay))) {
		uint64_t required_time_for_io =
			io_entry->size *
			(USEC_IN_SEC / bucket->max_iops_allowed);

		/* Under heavy IO load, and multiple exports, consider enough
		 * time looking backward for acutal IOPS calculation
		 * and consumption
		 **/
		if (((bucket->iops_ldct + required_time_for_io +
		      IOPS_EXPORT_FW_IO_SCHEDULE) > current_time)) {
			/* Resuming IOPS from last IO completion */
			bucket->iops_ldct =
				bucket->iops_ldct + required_time_for_io;
		} else {
			/* Resuming IOPS from IDLE */
			bucket->iops_ldct = current_time;
		}

		--bucket->num_ios_waiting;
		LogDebug(COMPONENT_QOS, "Timer entry resumed:%p", io_entry);
		io_entry->callback(io_entry->args);
		bucket->io_waitlist_qos_iops = io_entry->next;
		gsh_free(io_entry);
		io_entry = bucket->io_waitlist_qos_iops;
	}
	PTHREAD_MUTEX_unlock(&(bucket->lock));
}

/**
 * Function to initialize the QoS thread.
 * Will get called if qos is enabled after nfs_read_conf.
 */
void qos_init(void)
{
	PTHREAD_MUTEX_init(&g_qos_lock, NULL);
	if (qos_initalized == 0) {
		LogDebug(COMPONENT_QOS, "QOS thread_init");
		qos_thread_init();
	}
}
