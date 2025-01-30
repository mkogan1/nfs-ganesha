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
static inline void qos_bw_bucket_deffer_task(qos_bucket_t *bucket,
					     void *caller_data,
					     uint64_t timeout, uint64_t size,
					     unsigned int op_type);
static inline void release_wait_ios(timer_entry_t **head,
				    unsigned int *counter1,
				    unsigned int *counter2);
static inline void pspc_free_client_list(qos_client_t **head);
qos_client_t *pspc_remove_client_from_list(qos_client_t **head,
					   sockaddr_t *client_addr);

qos_share_t *get_share_qos(struct gsh_export *export);
qos_client_t *get_client_qos(struct gsh_client *client);

#define THREAD_DELAY_NFS_ERR_DELAY_DEFAULT 15
#define THREAD_DELAY_NFS_ERR_DELAY_IMMED 1

/* Currently BW controlling using SYNC is disabled
 * This is compile time config */
#define BW_SYNC_ENABLE 0
#define BW_ASYNC_ENABLE !BW_SYNC_ENABLE

qos_block_config_t qos_block_config;
qos_block_config_t *g_qos_config = (qos_block_config_t *)&qos_block_config;

#define DELAY_MSEC 1000
#define BW_DELAY_MSEC 2
#define BW_DELAY_USEC (BW_DELAY_MSEC * 1000)

/* Share level IO will be pushed down for future 5msec
 * Ensures even at heavy load, qos thread able to process enough IO's */
#define BW_SHARE_FW_IO_SCHEDULE (BW_DELAY_USEC * 5)

/* Client level IO will be rescheduled to share bucket till
 * current time + 5 times the BW_SHARE_FW_IO_SCHEDULE
 * This ensures even at load time enough IO's are schedules
 * from client bucket to share bucket in one iteration*/
#define BW_CLIENT_FW_IO_SCHEDULE (BW_SHARE_FW_IO_SCHEDULE * 5)

/*  Indicates token refersh should happen every 1 sec */
#define TOKEN_REFRESH_DELAY (DELAY_MSEC / BW_DELAY_MSEC)

pthread_mutex_t g_qos_lock = PTHREAD_MUTEX_INITIALIZER;

static inline void qos_drain_token_ios(void *qos_class,
				       unsigned int qos_class_type)
{
	qos_client_entry_t *token_client = NULL;
	qos_client_entry_t *temp = NULL;
	uint32_t *num_ios_waiting = NULL;
	bool token_enabled = false;

	if (qos_class_type == QOS_SHARE) {
		token_enabled = ((qos_share_t *)qos_class)->token_enabled;
		token_client = ((qos_share_t *)qos_class)->client_entries;
		num_ios_waiting =
			&(((qos_share_t *)qos_class)->num_ios_waiting);

	} else {
		token_enabled = ((qos_client_t *)qos_class)->token_enabled;
		token_client = ((qos_client_t *)qos_class)->client_entries;
		num_ios_waiting =
			&(((qos_client_t *)qos_class)->num_ios_waiting);
	}
	if (token_enabled && token_client != NULL) {
		while (token_client != NULL) {
			temp = token_client->next;
			LogFullDebug(COMPONENT_QOS,
				     "token clients present:%d:%p:%d",
				     g_qos_config->qos_type, token_client,
				     token_client->num_ios_waiting);
			release_wait_ios(&(token_client->io_waitlist_qos),
					 num_ios_waiting,
					 &(token_client->num_ios_waiting));
			token_client = temp;
		}
	}
}

void qos_drain_bw_ios(void *qos_class, unsigned int qos_class_type)
{
	bool bw_enabled = false;
	qos_bucket_t *rbucket =
		qos_get_bw_bucket(qos_class, qos_class_type, QOS_READ);
	qos_bucket_t *wbucket =
		qos_get_bw_bucket(qos_class, qos_class_type, QOS_WRITE);
	int dummy_counter = 0;

	if (qos_class_type == QOS_SHARE) {
		bw_enabled = ((qos_share_t *)qos_class)->bw_enabled;
		((qos_share_t *)qos_class)->bw_enabled = 0;
	} else {
		bw_enabled = ((qos_client_t *)qos_class)->bw_enabled;
		((qos_client_t *)qos_class)->bw_enabled = 0;
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

bool pspc_per_export_free_mem_cb(struct gsh_export *export, void *state)
{
	if (export == NULL)
		return true;

	qos_share_t *s_qos_class = export->qos_class;

	/* list is not populated */
	if (s_qos_class == NULL || s_qos_class->clients == NULL)
		return true;

	qos_client_t *c_qos_class = pspc_remove_client_from_list(
		&(s_qos_class->clients), (sockaddr_t *)state);

	if (c_qos_class != NULL) {
		LogFullDebug(COMPONENT_QOS,
			     "Tried freeing client:%d from export mem :%p",
			     export->qos_class->share_id, (sockaddr_t *)state);
		qos_drain_token_ios(c_qos_class, QOS_CLIENT);
		qos_drain_bw_ios(c_qos_class, QOS_CLIENT);
		gsh_free(c_qos_class);
	} else {
		LogFullDebug(COMPONENT_QOS,
			     "Tried freeing client:%d from export mem :%p",
			     export->qos_class->share_id, (sockaddr_t *)state);
	}
	/* Continue the Iteration for next share */
	return true;
}

void qos_free_mem(void *gsh_ptr, unsigned int qos_class_type)
{
	struct gsh_export *export = gsh_ptr;
	struct gsh_client *client = gsh_ptr;

	if (qos_class_type == QOS_SHARE) {
		if (export == NULL || export->qos_class == NULL)
			return;
	} else {
		if (client == NULL)
			return;
	}

	switch (g_qos_config->qos_type) {
	case QOS_PS_ENABLED:
		if (qos_class_type == QOS_SHARE) {
			qos_drain_token_ios(export->qos_class, QOS_SHARE);
			qos_drain_bw_ios(export->qos_class, QOS_SHARE);
			LogFullDebug(COMPONENT_QOS, "freeing export mem :%d",
				     export->qos_class->share_id);
			gsh_free(export->qos_class);
			if (export->qos_block)
				gsh_free(export->qos_block);
		}
		break;
	case QOS_PC_ENABLED:
		if (qos_class_type == QOS_CLIENT && client->qos_class != NULL) {
			LogFullDebug(COMPONENT_QOS, "freeing client mem :%p",
				     client->qos_class->client_addr);
			qos_drain_token_ios(client->qos_class, QOS_CLIENT);
			qos_drain_bw_ios(client->qos_class, QOS_CLIENT);
			gsh_free(client->qos_class);
		}
		break;
	case QOS_PS_PC_ENABLED:
		if (qos_class_type == QOS_SHARE) {
			struct QoS_perShare_Class *s_qos_class =
				export->qos_class;
			qos_client_t *c_qos_class = s_qos_class->clients;

			LogFullDebug(COMPONENT_QOS, "freeing export mem :%d",
				     export->qos_class->share_id);
			/* releasing BW waiting io's */
			while (c_qos_class != NULL) {
				qos_drain_token_ios(c_qos_class, QOS_CLIENT);
				qos_drain_bw_ios(c_qos_class, QOS_CLIENT);
				c_qos_class = c_qos_class->next;
			}
			pspc_free_client_list(&s_qos_class->clients);
			qos_drain_token_ios(s_qos_class, QOS_SHARE);
			qos_drain_bw_ios(s_qos_class, QOS_SHARE);
			gsh_free(export->qos_class);
			if (export->qos_block)
				gsh_free(export->qos_block);
		} else {
			LogFullDebug(COMPONENT_QOS,
				     "freeing client:%p from all export",
				     &(client->cl_addrbuf));
			foreach_gsh_export(pspc_per_export_free_mem_cb, false,
					   &(client->cl_addrbuf));
		}
		break;
	default:
		LogFullDebug(COMPONENT_QOS, " Something really wrong:%d",
			     g_qos_config->qos_type);
	}
}

void copy_gsh_qos_mem(struct gsh_export *dest, struct gsh_export *src)
{
	if (g_qos_config->enable_qos == false)
		return;

	switch (g_qos_config->qos_type) {
	case QOS_PS_ENABLED:
		qos_block_config_t *new_values_ps = NULL;

		if (src->qos_block != NULL) {
			if (dest->qos_block != NULL) {
				memcpy(dest->qos_block, src->qos_block,
				       sizeof(qos_block_config_t));
				/*  Set the new values to the qos_class
				 *  using new dest->qos_block value */
				new_values_ps = dest->qos_block;
			} else {
				/* Refelect the new values to share
				 * and new struct will be freed
				 * newly provided qos_block in config file*/
				new_values_ps = src->qos_block;
			}
		} else {
			/* Refelect the global values to share*/
			new_values_ps = g_qos_config;
		}
		LogFullDebug(COMPONENT_QOS, "eid:%d sb:%p dp:%p gv:%p nv:%p",
			     dest->export_id, src->qos_block, dest->qos_block,
			     g_qos_config, new_values_ps);
		QoS_perShareInsert(dest, new_values_ps);

		break;
	case QOS_PC_ENABLED:
		LogFullDebug(COMPONENT_QOS, " NOT expected call here");
		break;
	case QOS_PS_PC_ENABLED:
		qos_block_config_t *new_values_pspc = NULL;
		qos_share_t *qos_share = NULL;
		qos_client_t *qos_client = NULL;

		if (src->qos_block != NULL) {
			if (dest->qos_block != NULL) {
				memcpy(dest->qos_block, src->qos_block,
				       sizeof(qos_block_config_t));
				/*  Set the new values to the qos_class using
				 *  new dest->qos_block value */
				new_values_pspc = dest->qos_block;
			} else {
				new_values_pspc = src->qos_block;
			}
		} else {
			new_values_pspc = g_qos_config;
		}
		LogFullDebug(COMPONENT_QOS, "eid:%d sb:%p dp:%p gv:%p nv:%p",
			     dest->export_id, src->qos_block, dest->qos_block,
			     g_qos_config, new_values_pspc);
		QoS_perShareInsert(dest, new_values_pspc);
		qos_share = dest->qos_class;
		pthread_mutex_lock(&qos_share->lock);
		qos_client = qos_share->clients;
		while (qos_client) {
			setNode_pc(qos_client, qos_client->client_addr,
				   dest->qos_block);
			qos_client = qos_client->next;
		}
		pthread_mutex_unlock(&qos_share->lock);
		break;
	default:
		LogFullDebug(COMPONENT_QOS, " Something really wrong:%d",
			     g_qos_config->qos_type);
	}
}
static void set_bucket_value(qos_bucket_t *bucket, unsigned int max_bw,
			     unsigned int max_tokens,
			     unsigned int tokens_renew_time)
{
	bucket->max_bw_allowed = max_bw;
	bucket->max_available_tokens = max_tokens;
	bucket->tokens_renew_time = (tokens_renew_time * 1000000);
}

static void print_bucket_values(qos_bucket_t *bucket)
{
	LogFullDebug(
		COMPONENT_QOS,
		"wio:%d bw:%ld bw_ldct:%ld mat:%ld tc:%ld trt:%ld ltct:%ld",
		bucket->num_ios_waiting, bucket->max_bw_allowed,
		bucket->bw_ldct, bucket->max_available_tokens,
		bucket->tokens_consumed, bucket->tokens_renew_time,
		bucket->last_tokens_consumed_time);
}
static inline void print_class_values(void *qos_class,
				      unsigned int qos_class_type,
				      const char *str)
{
	if (qos_class_type == QOS_SHARE) {
		qos_share_t *share = qos_class;

		LogFullDebug(COMPONENT_QOS,
			     "%s SI:%d s_wio:%d bw_e:%d t_e:%d c_bw:%d c_rw:%d",
			     str, share->share_id, share->num_ios_waiting,
			     share->bw_enabled, share->token_enabled,
			     share->combined_rw_bw_control,
			     share->combined_rw_token_control);
		print_bucket_values(&(share->read_bucket));
		print_bucket_values(&(share->write_bucket));

	} else if (qos_class_type == QOS_CLIENT) {
		qos_client_t *client = qos_class;

		LogFullDebug(COMPONENT_QOS,
			     "%s CI:%p s_wio:%d bw_e:%d t_e:%d c_bw:%d c_t:%d",
			     str, client->client_addr, client->num_ios_waiting,
			     client->bw_enabled, client->token_enabled,
			     client->combined_rw_bw_control,
			     client->combined_rw_token_control);

		print_bucket_values(&(client->read_bucket));
		print_bucket_values(&(client->write_bucket));
	}
}
static void set_bucket_values(void *entry, unsigned int class_type,
			      struct qos_block_config *in)
{
	if (in->enable_qos == false)
		return;

	if (class_type == QOS_SHARE) {
		qos_share_t *share_entry = entry;

		if ((g_qos_config->enable_tokens && in->enable_tokens) &&
		    (g_qos_config->enable_bw_control &&
		     in->enable_bw_control)) {
			share_entry->bw_enabled = 1;
			share_entry->token_enabled = 1;
			set_bucket_value(&(share_entry->read_bucket),
					 in->max_export_read_bw,
					 in->max_export_read_tokens,
					 in->export_read_tokens_renew_time);
			set_bucket_value(&(share_entry->write_bucket),
					 in->max_export_write_bw,
					 in->max_export_write_tokens,
					 in->export_write_tokens_renew_time);
		} else if (g_qos_config->enable_bw_control &&
			   in->enable_bw_control) {
			share_entry->bw_enabled = 1;
			set_bucket_value(&(share_entry->read_bucket),
					 in->max_export_read_bw, 0, 0);
			set_bucket_value(&(share_entry->write_bucket),
					 in->max_export_write_bw, 0, 0);
		} else if (g_qos_config->enable_tokens && in->enable_tokens) {
			share_entry->token_enabled = 1;
			set_bucket_value(&(share_entry->read_bucket), 0,
					 in->max_export_read_tokens,
					 in->export_read_tokens_renew_time);
			set_bucket_value(&(share_entry->write_bucket), 0,
					 in->max_export_write_tokens,
					 in->export_write_tokens_renew_time);
		}
		print_class_values(share_entry, QOS_SHARE, "debugdp");
	} else {
		qos_client_t *client_entry = entry;

		if ((g_qos_config->enable_tokens && in->enable_tokens) &&
		    (g_qos_config->enable_bw_control &&
		     in->enable_bw_control)) {
			client_entry->bw_enabled = 1;
			client_entry->token_enabled = 1;
			set_bucket_value(&(client_entry->read_bucket),
					 in->max_client_read_bw,
					 in->max_client_read_tokens,
					 in->client_read_tokens_renew_time);
			set_bucket_value(&(client_entry->write_bucket),
					 in->max_client_write_bw,
					 in->max_client_write_tokens,
					 in->client_write_tokens_renew_time);
		} else if (g_qos_config->enable_bw_control &&
			   in->enable_bw_control) {
			client_entry->bw_enabled = 1;
			set_bucket_value(&(client_entry->read_bucket),
					 in->max_client_read_bw, 0, 0);
			set_bucket_value(&(client_entry->write_bucket),
					 in->max_client_write_bw, 0, 0);
		} else if (g_qos_config->enable_tokens && in->enable_tokens) {
			client_entry->token_enabled = 1;
			set_bucket_value(&(client_entry->read_bucket), 0,
					 in->client_read_tokens_renew_time,
					 in->max_client_read_tokens);
			set_bucket_value(&(client_entry->write_bucket), 0,
					 in->client_write_tokens_renew_time,
					 in->max_client_write_tokens);
		}
		print_class_values(client_entry, QOS_CLIENT, "debugdp");
	}
}

static void setNode_ps(qos_share_t *node, uint16_t export_id,
		       struct qos_block_config *qos_block)
{
	node->share_id = export_id;
	node->combined_rw_bw_control = qos_block->combined_rw_bw_control;
	node->combined_rw_token_control = qos_block->combined_rw_token_control;
	if (qos_block->combined_rw_bw_control) {
		qos_block->max_export_write_bw =
			qos_block->max_export_combined_bw;
		qos_block->max_client_write_bw =
			qos_block->max_client_combined_bw;
	}
	set_bucket_values(node, QOS_SHARE, qos_block);
}

static void setNode_pc(qos_client_t *node, sockaddr_t *client_addr,
		       struct qos_block_config *qos_block)
{
	node->client_addr = client_addr;
	node->combined_rw_bw_control = qos_block->combined_rw_bw_control;
	node->combined_rw_token_control = qos_block->combined_rw_token_control;
	if (qos_block->combined_rw_bw_control) {
		qos_block->max_export_write_bw =
			qos_block->max_export_combined_bw;
		qos_block->max_client_write_bw =
			qos_block->max_client_combined_bw;
	}
	set_bucket_values(node, QOS_CLIENT, qos_block);
}

void QoS_perShareInsert(struct gsh_export *export,
			struct qos_block_config *qos_block)
{
	struct qos_block_config *lqos_block = NULL;

	if (qos_block != NULL)
		lqos_block = qos_block;
	else
		lqos_block = g_qos_config;

	/*  Condition indicates this is new export, reexport or
	 *  run time enabled of QOS due to global config */
	if (export->qos_class == NULL) {
		qos_share_t *node = gsh_malloc(sizeof(qos_share_t));

		memset(node, 0, sizeof(qos_share_t));
		/* NULL Indicates QOS block is not popultaed
		 * i.e run time enabledment of QOS */
		if (export->qos_block == NULL) {
			qos_block_config_t *new_block;

			new_block = gsh_malloc(sizeof(qos_block_config_t));
			memset(new_block, 0, sizeof(qos_block_config_t));
			*new_block = *lqos_block;
			export->qos_block = new_block;
		}
		pthread_mutex_init(&(node->lock), NULL);
		pthread_mutex_init(&(node->read_bucket.lock), NULL);
		pthread_mutex_init(&(node->write_bucket.lock), NULL);
		export->qos_class = node;
	}

	setNode_ps(export->qos_class, export->export_id, lqos_block);
	LogFullDebug(COMPONENT_QOS, "Config update for :%s",
		     export->cfg_fullpath);
}

qos_client_t *allocate_client(void)
{
	qos_client_t *node = gsh_malloc(sizeof(qos_client_t));

	memset(node, 0, sizeof(qos_client_t));
	pthread_mutex_init(&(node->lock), NULL);
	pthread_mutex_init(&(node->read_bucket.lock), NULL);
	pthread_mutex_init(&(node->write_bucket.lock), NULL);
	return node;
}
qos_client_t *pspc_allocate_and_init_client(sockaddr_t *client_addr,
					    struct qos_block_config *qos_block)
{
	qos_client_t *new_node = allocate_client();

	if (new_node)
		setNode_pc(new_node, client_addr, qos_block);

	return new_node;
}

void pspc_add_client_to_list(qos_client_t **head, qos_client_t *client)
{
	if (!client)
		return;

	client->next = *head;
	*head = client;
}

qos_client_t *pspc_alloc_init_add_client(qos_client_t **head,
					 sockaddr_t *client_addr,
					 struct qos_block_config *qos_block)
{
	qos_client_t *new_node =
		pspc_allocate_and_init_client(client_addr, qos_block);

	if (new_node)
		pspc_add_client_to_list(head, new_node);

	return new_node;
}

qos_client_t *pspc_get_client_from_list(qos_client_t *head,
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

qos_client_t *pspc_remove_client_from_list(qos_client_t **head,
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
/*  Free all the clients related to a share */
static inline void pspc_free_client_list(qos_client_t **head)
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

static inline uint64_t get_time_in_usec(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	/* Convert to uSeconds and return */
	return ((ts.tv_sec * 1000000) + (ts.tv_nsec / 1000));
}

static inline qos_bucket_t *qos_get_bucket(void *entry, unsigned int class_type,
					   unsigned int op_type)
{
	if (class_type == QOS_SHARE || class_type == QOS_PSPC)
		return (op_type == QOS_READ) ?
			       &((qos_share_t *)entry)->read_bucket :
			       &((qos_share_t *)entry)->write_bucket;
	else
		return (op_type == QOS_READ) ?
			       &((qos_client_t *)entry)->read_bucket :
			       &((qos_client_t *)entry)->write_bucket;
}

/*  Manupulte this function to make single bucket or
 *  independent read/write bucket for BandWidth Control*/
static inline qos_bucket_t *qos_get_token_bucket(void *qos_class,
						 unsigned int class_type,
						 unsigned int op_type)
{
	if (class_type == QOS_SHARE || class_type == QOS_PSPC) {
		qos_share_t *share = qos_class;

		if (share->token_enabled == 0)
			return NULL;
		if (share->combined_rw_token_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(share, class_type, op_type);
	} else {
		qos_client_t *client = qos_class;

		if (client->token_enabled == 0)
			return NULL;
		if (client->combined_rw_token_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(client, class_type, op_type);
	}
}

/*  Manupulte this function to make single bucket or
 *  independent read/write bucket for Token Control */
static inline qos_bucket_t *qos_get_bw_bucket(void *qos_class,
					      unsigned int class_type,
					      unsigned int op_type)
{
	if (class_type == QOS_SHARE || class_type == QOS_PSPC) {
		qos_share_t *share = qos_class;

		if (share->bw_enabled == 0)
			return NULL;
		if (share->combined_rw_bw_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(share, class_type, op_type);
	} else {
		qos_client_t *client = qos_class;

		if (client->bw_enabled == 0)
			return NULL;
		if (client->combined_rw_bw_control)
			op_type = QOS_WRITE;
		return qos_get_bucket(client, class_type, op_type);
	}
}

/*  True indicates : consumed the token for the current io
 *  False indicates : Not able to consume token i.,e tokens alreday exuhasted
 **/
static bool qos_check_bucket_token_availability(qos_bucket_t *bucket,
						uint64_t request_size)
{
	if (bucket->tokens_consumed <= bucket->max_available_tokens)
		return true;
	else
		return false;
}

static void qos_consume_bucket_token(qos_bucket_t *bucket,
				     uint64_t request_size)
{
	bucket->last_tokens_consumed_time = get_time_in_usec();
	bucket->tokens_consumed += request_size;
}

/*  True indicates : consumed the token for the current io
 *			or was not suppose to consume token
 *  False indicates : Not able to consume token i.e tokens alreday exuhasted
 **/
static bool qos_check_token_availability(void *qos_class, uint64_t request_size,
					 unsigned int op_type,
					 unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return true;
	return qos_check_bucket_token_availability(bucket, request_size);
}

static void qos_consume_token(void *qos_class, uint64_t request_size,
			      unsigned int op_type, unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return;
	return qos_consume_bucket_token(bucket, request_size);
}

/* return true i.e ASYNC scheduled
 * return false on SYNC delay or BW is not enabled
 */
static bool qos_control_bucket_bw(qos_bucket_t *bucket, uint64_t request_size,
				  unsigned int op_type, void *caller_data)
{
	LogFullDebug(COMPONENT_QOS, "Max_bw_io :%ld BW_control_type:%d ",
		     bucket->max_bw_allowed, BW_SYNC_ENABLE);
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
static bool qos_control_bw(void *qos_class, uint64_t request_size,
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

static inline void qos_bw_deffer_task(void *qos_class, void *caller_data,
				      uint64_t size, uint64_t timeout,
				      unsigned int op_type,
				      unsigned int class_type)
{
	qos_bucket_t *bucket =
		qos_get_bw_bucket(qos_class, class_type, op_type);

	if (bucket == NULL)
		return;

	LogFullDebug(COMPONENT_QOS, "going for deffer task");
	return qos_bw_bucket_deffer_task(bucket, caller_data, size, timeout,
					 op_type);
}

void qos_thread_check(void)
{
	if (qos_initalized == 0) {
		LogFullDebug(COMPONENT_QOS, "QOS thread_init");
		qos_thread_init();
	}
}

static bool qos_check_ps(void *class_ptr, uint64_t request_size,
			 unsigned int op_type, void *caller_data,
			 compound_data_t *data, unsigned int class_type)
{
	qos_share_t *qos_class = class_ptr;

	qos_thread_check();
	pthread_mutex_lock(&qos_class->lock);
	if (!qos_check_token_availability(qos_class, request_size, op_type,
					  QOS_SHARE)) {
		qos_token_exausted_deffer_task(qos_class, caller_data, data,
					       QOS_SHARE, op_type);
		pthread_mutex_unlock(&qos_class->lock);
		return false;
	} else if (!qos_control_bw(qos_class, request_size, op_type,
				   caller_data, QOS_SHARE)) {
		/*  Consume the ASYNC scheduled tokens */
		qos_consume_token(qos_class, request_size, op_type, QOS_SHARE);
		pthread_mutex_unlock(&qos_class->lock);
		return false;
	}
	qos_consume_token(qos_class, request_size, op_type, QOS_SHARE);
	pthread_mutex_unlock(&qos_class->lock);
	return true;
}
static bool qos_check_pc(void *class_ptr, uint64_t request_size,
			 unsigned int op_type, void *caller_data,
			 compound_data_t *data, unsigned int class_type)
{
	qos_client_t *qos_class = class_ptr;

	qos_thread_check();
	pthread_mutex_lock(&qos_class->lock);
	if (!qos_check_token_availability(qos_class, request_size, op_type,
					  QOS_CLIENT)) {
		qos_token_exausted_deffer_task(qos_class, caller_data, data,
					       QOS_CLIENT, op_type);
		pthread_mutex_unlock(&qos_class->lock);
		return false;
	} else if (!qos_control_bw(qos_class, request_size, op_type,
				   caller_data, QOS_CLIENT)) {
		qos_consume_token(qos_class, request_size, op_type, QOS_CLIENT);
		pthread_mutex_unlock(&qos_class->lock);
		return false;
	}
	qos_consume_token(qos_class, request_size, op_type, QOS_CLIENT);
	pthread_mutex_unlock(&qos_class->lock);
	return true;
}
static bool qos_check_pspc(void *class_ptr, uint64_t request_size,
			   unsigned int op_type, void *caller_data,
			   compound_data_t *data, unsigned int class_type)
{
	qos_share_t *s_qos_class = class_ptr;
	qos_client_t *c_qos_class = pspc_get_client_from_list(
		s_qos_class->clients, &op_ctx->client->cl_addrbuf);

	int share_token_available = qos_check_token_availability(
		s_qos_class, request_size, op_type, QOS_SHARE);
	int client_token_available = qos_check_token_availability(
		c_qos_class, request_size, op_type, QOS_CLIENT);

	qos_thread_check();
	/*  here for accounting info take sharelevel lock,
	 *  which will aloow us to handling the runtime disablement
	 *  and enablement of QOS BW and Token control.
	 *  IO Consumer thread will work on bucket locks */
	pthread_mutex_lock(&s_qos_class->lock);
	LogFullDebug(COMPONENT_QOS, "Inside the lock");
	if (!share_token_available) {
		qos_token_exausted_deffer_task(s_qos_class, caller_data, data,
					       QOS_SHARE, op_type);
		pthread_mutex_unlock(&s_qos_class->lock);
		return false;
	} else if (!client_token_available) {
		qos_token_exausted_deffer_task(c_qos_class, caller_data, data,
					       QOS_CLIENT, op_type);
		pthread_mutex_unlock(&s_qos_class->lock);
		return false;
	} else {
		/*  consume the Tokens and schedule for ASYNC in the
		 *  client buckets, rescheuling of IO to share bucket
		 *  will happend later.
		 *  BW limits also decided later */
		qos_consume_token(s_qos_class, request_size, op_type,
				  QOS_SHARE);
		qos_consume_token(c_qos_class, request_size, op_type,
				  QOS_CLIENT);
		LogFullDebug(COMPONENT_QOS, "bw enabled check");
		if (c_qos_class->bw_enabled) {
			LogFullDebug(COMPONENT_QOS, "deffering task ");
			qos_bw_deffer_task(c_qos_class, caller_data,
					   request_size, get_time_in_usec(),
					   op_type, QOS_CLIENT);
			pthread_mutex_unlock(&s_qos_class->lock);
			return false;
		} else {
			pthread_mutex_unlock(&s_qos_class->lock);
			return true;
		}
	}
	return true;
}

unsigned int QoS_Process_ps(unsigned int size, void *caller_data,
			    compound_data_t *data, unsigned int op_type)
{
	if (op_ctx->ctx_export->qos_class == NULL) {
		pthread_mutex_lock(&g_qos_lock);
		LogFullDebug(COMPONENT_QOS,
			     "PS key not found for:%s, so creating new entry",
			     op_ctx->ctx_export->cfg_fullpath);

		if (op_ctx->ctx_export->qos_class == NULL)
			QoS_perShareInsert(op_ctx->ctx_export, g_qos_config);

		pthread_mutex_unlock(&g_qos_lock);
	}
	if (!qos_check_ps(op_ctx->ctx_export->qos_class, size, op_type,
			  caller_data, data, QOS_SHARE)) {
		return 1;
	}
	return 0;
}
unsigned int QoS_Process_pc(unsigned int size, void *caller_data,
			    compound_data_t *data, unsigned int op_type)
{
	if (op_ctx->client->qos_class == NULL) {
		pthread_mutex_lock(&g_qos_lock);
		LogFullDebug(
			COMPONENT_QOS,
			"PC client entry not found :%p, creating new client",
			&op_ctx->client->cl_addrbuf);

		/* Since this is QOS_PC, pass the global QOS values */
		if (op_ctx->client->qos_class == NULL)
			QoS_perClientInsert(g_qos_config, op_ctx->client);

		pthread_mutex_unlock(&g_qos_lock);
	}
	if (!qos_check_pc(op_ctx->client->qos_class, size, op_type, caller_data,
			  data, QOS_CLIENT)) {
		return 1;
	}
	return 0;
}
unsigned int QoS_Process_pspc(unsigned int size, void *caller_data,
			      compound_data_t *data, unsigned int op_type)
{
	char *key = op_ctx->ctx_export->cfg_fullpath;
	qos_share_t *share = op_ctx->ctx_export->qos_class;
	sockaddr_t *client_addr = &op_ctx->client->cl_addrbuf;

	if (share == NULL) {
		/*  Execution reached here means QOS is enabled but,
		 *  QOS block is not populated for this share
		 *  so apply the global values to the share values
		 *  and mark the qos_enabled to false, we need this in case of
		 *  runtime enablement of QOS*/
		pthread_mutex_lock(&g_qos_lock);
		/* On runtime enablement of QOS, or due to only global config
		 * present, there is possiblity of multiple IO's rushing
		 * into this function for this paticular share,
		 * so recheck before going for allocation*/
		share = op_ctx->ctx_export->qos_class;
		if (share == NULL) {
			QoS_perShareInsert(op_ctx->ctx_export, g_qos_config);
			share = op_ctx->ctx_export->qos_class;
		}
		pthread_mutex_unlock(&g_qos_lock);
	}
	/* Is QOS disabled for this particular share */
	if (share->bw_enabled || share->token_enabled) {
		qos_client_t *client = NULL;

		client = pspc_get_client_from_list(share->clients, client_addr);
		if (client == NULL) {
			LogFullDebug(COMPONENT_QOS,
				     "Share:%s Client not found: %p", key,
				     client_addr);
			pthread_mutex_lock(&share->lock);
			client = pspc_get_client_from_list(share->clients,
							   client_addr);
			if (client == NULL)
				client = pspc_alloc_init_add_client(
					&(share->clients), client_addr,
					op_ctx->ctx_export->qos_block);
			pthread_mutex_unlock(&share->lock);
		}

		LogFullDebug(COMPONENT_QOS, "PerShare key found :%s", key);
		if (!qos_check_pspc(share, size, op_type, caller_data, data,
				    QOS_PSPC)) {
			return 1;
		}
	}
	return 0;
}

/* On IO differed/rescheduled by QOS will return true else false  */
unsigned int QoS_Process(unsigned int size, void *caller_data,
			 compound_data_t *data, unsigned int op_type)
{
	unsigned int ret = 0;

	if (g_qos_config->qos_type == QOS_NOT_ENABLED ||
	    g_qos_config->enable_qos == 0) {
		ret = 0;
	} else if (g_qos_config->qos_type == QOS_PS_ENABLED) {
		ret = QoS_Process_ps(size, caller_data, data, op_type);
	} else if (g_qos_config->qos_type == QOS_PC_ENABLED) {
		ret = QoS_Process_pc(size, caller_data, data, op_type);
	} else if (g_qos_config->qos_type == QOS_PS_PC_ENABLED) {
		ret = QoS_Process_pspc(size, caller_data, data, op_type);
	} else {
		LogFullDebug(COMPONENT_QOS, " INVALID QOS_TYPE:%d",
			     g_qos_config->qos_type);
	}
	return ret;
}

qos_svc_rcb get_qos_resume_cb(unsigned int op_type)
{
	return (op_type == QOS_READ) ? nfs4_qos_read_cb : nfs4_qos_write_cb;
}

uint64_t qos_get_time_to_tokenrefresh(void *qos_class, unsigned int class_type,
				      unsigned int op_type, uint64_t ctime)
{
	qos_bucket_t *bucket =
		qos_get_token_bucket(qos_class, class_type, op_type);
	uint64_t ret = (((bucket->last_tokens_consumed_time +
			  bucket->tokens_renew_time) -
			 ctime) /
			1000000);
	LogFullDebug(COMPONENT_QOS, "LTC:%ld TRT:%ld CT:%ld TO:%ld",
		     bucket->last_tokens_consumed_time,
		     bucket->tokens_renew_time, ctime, ret);

	return ret;
}

static inline struct qos_op_cb_arg *alloc_qos_cb_args(void *caller_data,
						      int ratecontrol)
{
	struct qos_op_cb_arg *qos_cb_args = NULL;

	qos_cb_args = gsh_malloc(sizeof(struct qos_op_cb_arg));
	memset(qos_cb_args, 0, sizeof(struct qos_op_cb_arg));
	qos_cb_args->caller_data = caller_data;
	qos_cb_args->ratecontrol = ratecontrol;
	return qos_cb_args;
}

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

	if (class_type == QOS_SHARE) {
		client = get_and_insert_client_details(
			&(((qos_share_t *)ptr)->client_entries), data);
		num_ios_waiting = &(((qos_share_t *)ptr)->num_ios_waiting);
	} else {
		client = get_and_insert_client_details(
			&(((qos_client_t *)ptr)->client_entries), data);
		num_ios_waiting = &(((qos_client_t *)ptr)->num_ios_waiting);
	}

	if (client->num_ios_waiting >= 5 && client->epoll_disabled == 0) {
		client->epoll_disabled = 1;
		LogFullDebug(COMPONENT_QOS,
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

	LogFullDebug(COMPONENT_QOS,
		     "new_timer %p io_w:%d c_io_w:%d CI:%p CT:%ld TO:%ld",
		     new_timer_entry, *num_ios_waiting, client->num_ios_waiting,
		     client->client_addr, ltime, timeout);
}

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
	pthread_mutex_lock(&bucket->lock);
	insert_timer_entry(&(bucket->io_waitlist_qos_bc), new_timer_entry);
	++bucket->num_ios_waiting;
	pthread_mutex_unlock(&bucket->lock);
}

/*function Controls bandwidth NON-BLOCKING-IO:
 *		Check bandwidth and delay if necessary */
static inline bool check_bandwidth_and_reschedule(qos_bucket_t *bucket,
						  uint64_t bytes,
						  void *caller_data,
						  unsigned int op_type)
{
	uint64_t last_time = bucket->bw_ldct;
	uint64_t current_time = get_time_in_usec();
	/* Microseconds required to meet bandwidth */
	uint64_t required_time = (bytes * 1000000) / bucket->max_bw_allowed;

	LogFullDebug(COMPONENT_QOS, "ct:%ld bw_ldct:%ld rt:%ld bytes:%ld",
		     current_time, last_time, required_time, bytes);
	/* Condition will be true only after IDLE, or most of the time else */
	if (current_time > last_time) {
		/* Microseconds elapsed since last call */
		uint64_t time_since_last_op = current_time - last_time;

		LogFullDebug(COMPONENT_QOS, "tslo:%ld rt:%ld",
			     time_since_last_op, required_time);
		if (time_since_last_op < required_time) {
			/*i Calculate resume time in useconds and rescheudle */
			uint64_t resume_time =
				current_time +
				(required_time - time_since_last_op);
			LogFullDebug(
				COMPONENT_QOS,
				"ct:%ld, res_t:%ld rt:%ld tslo:%ld bw_ldct:%ld",
				current_time, resume_time, required_time,
				time_since_last_op, last_time);
			qos_bw_bucket_deffer_task(bucket, caller_data, bytes,
						  resume_time, op_type);
			bucket->bw_ldct = resume_time;
			return false;
		}
		bucket->bw_ldct = current_time + required_time;
		return true;
	} else {
		uint64_t resume_time = last_time + required_time;

		LogFullDebug(COMPONENT_QOS,
			     "ct:%ld, resumet:%ld rt:%ld bw_ldct:%ld",
			     current_time, resume_time, required_time,
			     last_time);
		qos_bw_bucket_deffer_task(bucket, caller_data, bytes,
					  resume_time, op_type);
		bucket->bw_ldct = resume_time;
		return false;
	}
}

/* function Controls bandwidth BLOCKING-IO :
 * Check bandwidth and delay if necessary
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
		LogFullDebug(
			COMPONENT_QOS,
			"ct:%ld dt:%ld rt:%ld tslo:%ld bw_ldct:%ld dis:%ld",
			current_time, delay_time, required_time,
			time_since_last_op, bucket->bw_ldct, delay.tv_sec);
		LogFullDebug(COMPONENT_QOS, "dins:%ld num_bytes:%ld wba:%ld",
			     delay.tv_nsec, bytes, bucket->max_bw_allowed);
		/* Enforce delay to limit bandwidth */
		ret = nanosleep(&delay, NULL);

		if (ret != 0)
			LogFullDebug(COMPONENT_QOS, "Sleep Failure ");
	}
	bucket->bw_ldct = get_time_in_usec();
	return true;
}

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
	    (ltime >
	     (bucket->last_tokens_consumed_time + bucket->tokens_renew_time))) {
		LogFullDebug(COMPONENT_QOS, "CT:%ld LCT:%ld TRT:%ld cal:%ld",
			     ltime, bucket->last_tokens_consumed_time,
			     bucket->tokens_renew_time,
			     (bucket->last_tokens_consumed_time +
			      bucket->tokens_renew_time));
		bucket->tokens_consumed = 0;
		return 1;
	} else {
		return 0;
	}
}

static inline bool refresh_per_share_tokens(qos_share_t *share_entry)
{
	/* since the io waitlist queue are same for read and write ios,
	 * check for both and then return */
	return (refresh_bucket_token(share_entry, QOS_SHARE, QOS_READ) ||
		refresh_bucket_token(share_entry, QOS_SHARE, QOS_WRITE));
}

static inline bool refresh_per_client_tokens(qos_client_t *client_entry)
{
	/* since the io waitlist queue are same for read and write ios,
	 * check for both and then return */
	return (refresh_bucket_token(client_entry, QOS_CLIENT, QOS_READ) ||
		refresh_bucket_token(client_entry, QOS_CLIENT, QOS_WRITE));
}

static inline timer_entry_t *
create_timer_entry(uint64_t expiry, void (*callback)(void *), void *args)
{
	timer_entry_t *new_entry = gsh_malloc(sizeof(timer_entry_t));

	memset(new_entry, 0, sizeof(timer_entry_t));
	new_entry->expiry = expiry;
	new_entry->callback = callback;
	new_entry->args = args;
	LogFullDebug(COMPONENT_QOS, "Timer entry created:%p", new_entry);
	return new_entry;
}

static qos_client_entry_t *alloc_clientdetails_ps(compound_data_t *data)
{
	qos_client_entry_t *new_entry = NULL;

	new_entry = gsh_malloc(sizeof(qos_client_entry_t));
	memset(new_entry, 0, sizeof(qos_client_entry_t));
	new_entry->client_addr = &op_ctx->client->cl_addrbuf;
	new_entry->data = data;
	new_entry->rq_xprt = data->req->rq_xprt;
	LogFullDebug(COMPONENT_QOS, "Adding Client entry CID:%p",
		     new_entry->client_addr);
	return new_entry;
}

static qos_client_entry_t *
get_and_insert_client_details(qos_client_entry_t **head, compound_data_t *data)
{
	qos_client_entry_t *new_entry = NULL;

	if (*head == NULL) {
		new_entry = alloc_clientdetails_ps(data);
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
			new_entry = alloc_clientdetails_ps(data);
			temp->next = new_entry;
			return new_entry;
		} else {
			return current;
		}
	}
}

static void remove_client_entry(qos_client_entry_t **head,
				qos_client_entry_t *entry_to_remove)
{
	LogFullDebug(COMPONENT_QOS,
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

static void insert_timer_entry(timer_entry_t **head, timer_entry_t *new_entry)
{
	if (new_entry == NULL)
		LogFullDebug(COMPONENT_QOS, "ERROR new entry is NULL");

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

static void remove_timer_entry(timer_entry_t **head,
			       timer_entry_t *entry_to_remove)
{
	LogFullDebug(COMPONENT_QOS, "Timer entry head:%p remove: %p", *head,
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

static void list_timer_entries(timer_entry_t *current_share_list)
{
	timer_entry_t *current = current_share_list;

	LogFullDebug(COMPONENT_QOS, "Current Timer Entries:");
	while (current != NULL) {
		LogFullDebug(COMPONENT_QOS,
			     "Entry:%p, Expiry: %ld, Callback: %p, Args: %p",
			     current, current->expiry,
			     (void *)current->callback, current->args);
		current = current->next;
	}
}
/*  Force resume all the waiting IO's of share
 *  Condition : Io's waiting for replinsh of token and tokens got replinsh
 *		Ignore the lease(expiry time) on IO.
 */
static inline void release_wait_ios(timer_entry_t **head,
				    unsigned int *counter1,
				    unsigned int *counter2)
{
	timer_entry_t *current = *head;
	timer_entry_t *expired = NULL;

	while (current != NULL) {
		current->callback(current->args);
		LogFullDebug(COMPONENT_QOS,
			     "Force resume Timer:%p Expiry:%ld TCounter:%d",
			     current, current->expiry, *counter1);
		expired = current;
		current = current->next;
		remove_timer_entry(head, expired);
		--*counter1;
		--*counter2;
	}
}

/*  Resume all the waiting IO's of share,
 *  based on the lease expiry time set by QOS.
 *  Expiry time on IO will be entertained
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
			if (counter1 != NULL) {
				LogFullDebug(COMPONENT_QOS,
					     "Exp_IO_T:%p CT:%ld Ex:%ld Tco:%d",
					     current, current_time,
					     current->expiry, *counter1);
			} else {
				LogFullDebug(
					COMPONENT_QOS,
					"Expired IO Timer:%p CT:%ld Expiry:%ld",
					current, current_time, current->expiry);
			}
			current->callback(current->args);
			expired = current;
		}
		current = current->next;
		if (expired != NULL) {
			remove_timer_entry(head, expired);
			if (counter1 != NULL && counter2 != NULL) {
				--*counter1;
				--*counter2;
			} else if (counter1 != NULL) {
				--*counter1;
			}
			expired = NULL;
		}
	}
}

static inline void refresh_qos_client(qos_client_entry_t **clients,
				      bool tokens_refreshed, void *qos_class,
				      unsigned int class_type)
{
	qos_client_entry_t *client = *clients;
	unsigned int *counter1 = &(client->num_ios_waiting);
	unsigned int *counter2 =
		(class_type == QOS_SHARE) ?
			&(((qos_share_t *)qos_class)->num_ios_waiting) :
			&(((qos_client_t *)qos_class)->num_ios_waiting);
	bool epd = 0;
	SVCXPRT *rq_xprt = NULL;

	LogFullDebug(COMPONENT_QOS, " CI:%p CWIO's:%d ", client->client_addr,
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
		LogFullDebug(
			COMPONENT_QOS,
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

static inline void refresh_qos_token_by_class(void *class,
					      unsigned int qos_class_type)
{
	bool tokens_refreshed = 0;
	qos_client_entry_t *client = NULL;
	qos_client_entry_t *temp = NULL;

	if (qos_class_type == QOS_SHARE) {
		qos_share_t *qos_class = class;

		tokens_refreshed = refresh_per_share_tokens(qos_class);
		LogFullDebug(COMPONENT_QOS, " SN:%d TR:%d WIO's:%d",
			     qos_class->share_id, tokens_refreshed,
			     qos_class->num_ios_waiting);
		pthread_mutex_lock(&(qos_class->lock));
		client = qos_class->client_entries;
		while (client != NULL) {
			temp = client->next;
			refresh_qos_client(&(qos_class->client_entries),
					   tokens_refreshed, qos_class,
					   QOS_SHARE);
			client = temp;
		}
		pthread_mutex_unlock(&(qos_class->lock));
	} else {
		qos_client_t *qos_class = class;

		tokens_refreshed = refresh_per_client_tokens(qos_class);
		LogFullDebug(COMPONENT_QOS, " CI:%p TR:%d WIO's:%d",
			     qos_class->client_addr, tokens_refreshed,
			     qos_class->num_ios_waiting);
		pthread_mutex_lock(&(qos_class->lock));
		client = qos_class->client_entries;
		while (client != NULL) {
			temp = client->next;
			refresh_qos_client(&(qos_class->client_entries),
					   tokens_refreshed, qos_class,
					   QOS_CLIENT);
			client = temp;
		}
		pthread_mutex_unlock(&(qos_class->lock));
	}
}

bool pspc_token_control_cb(struct gsh_export *export, void *state)
{
	qos_share_t *share = get_share_qos(export);

	if (share && share->token_enabled) {
		LogDebug(COMPONENT_QOS, "going for token refresh:%s",
			 export->cfg_fullpath);
		refresh_qos_token_by_class(share, QOS_SHARE);
		/* In a Share check any client exausted the token
		 * and is replinish value reached */
		qos_client_t *client = share->clients;

		while (client != NULL) {
			refresh_qos_token_by_class(client, QOS_CLIENT);
			client = client->next;
		}
	}
	/* Continue iteration */
	return true;
}

bool ps_token_control_cb(struct gsh_export *export, void *state)
{
	qos_share_t *share = get_share_qos(export);

	if (share && share->token_enabled) {
		LogDebug(COMPONENT_QOS, "going for token refresh:%s",
			 export->cfg_fullpath);
		refresh_qos_token_by_class(share, QOS_SHARE);
	}
	/* Continue iteration */
	return true;
}

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

static inline void refresh_qos_token(void)
{
	/*  Token refershing function calls here
	 *  Share based refresing
	 *  Client based refresing
	 *  PerShare-PerClient based refresing
	 *  Group Based
	 *  Directory level */
	int op_type = 0;

	switch (g_qos_config->qos_type) {
	case QOS_NOT_ENABLED:
		LogFullDebug(COMPONENT_QOS, "QOS not enabled :%d",
			     g_qos_config->qos_type);
		break;
	case QOS_PS_ENABLED:
		foreach_gsh_export(ps_token_control_cb, false, &op_type);
		break;
	case QOS_PC_ENABLED:
		foreach_gsh_client(pc_token_control_cb, &op_type);
		break;
	case QOS_PS_PC_ENABLED:
		foreach_gsh_export(pspc_token_control_cb, false, &op_type);
		break;
	default:
		LogFullDebug(COMPONENT_QOS, " Something really wrong:%d",
			     g_qos_config->qos_type);
		break;
	}
}

/*  Below all functions are resuming the asyncIO based on
 *  timeout set by BW calculation
 **/
static inline void resume_bw_bucket_io(qos_bucket_t *bucket)
{
	execute_qos_expired_timers(&(bucket->io_waitlist_qos_bc),
				   &(bucket->num_ios_waiting), NULL);
}

static inline void resume_bw_io_ps(qos_share_t *share, unsigned int op_type)
{
	if (share != NULL) {
		qos_bucket_t *bucket =
			qos_get_bw_bucket(share, QOS_SHARE, op_type);

		if (bucket == NULL)
			return;

		pthread_mutex_lock(&bucket->lock);
		resume_bw_bucket_io(bucket);
		pthread_mutex_unlock(&bucket->lock);
	}
}

static inline void resume_bw_io_pc(qos_client_t *client, unsigned int op_type)
{
	if (client != NULL) {
		qos_bucket_t *bucket =
			qos_get_bw_bucket(client, QOS_CLIENT, op_type);

		if (bucket == NULL)
			return;

		pthread_mutex_lock(&bucket->lock);
		resume_bw_bucket_io(bucket);
		pthread_mutex_unlock(&bucket->lock);
	}
}

static inline void resume_bw_io_pspc(qos_share_t *share, unsigned int op_type)
{
	if (share == NULL)
		return;

	uint64_t current_time = get_time_in_usec();
	int check_delay = ((op_type == QOS_READ) ? BW_SHARE_FW_IO_SCHEDULE :
						   BW_DELAY_USEC);
	qos_bucket_t *bucket = qos_get_bw_bucket(share, QOS_PSPC, op_type);

	if (bucket == NULL)
		return;

	pthread_mutex_lock(&(bucket->lock));
	timer_entry_t *io_entry = bucket->io_waitlist_qos_bc;

	LogFullDebug(COMPONENT_QOS,
		     ">>>> SI:%d s_wio:%d op_type:%d lct:%ld sb_io:%d ",
		     share->share_id, share->num_ios_waiting, op_type,
		     bucket->bw_ldct, bucket->num_ios_waiting);
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
		      BW_SHARE_FW_IO_SCHEDULE) > current_time)) {
			/* Resuming BW from last IO completion */
			bucket->bw_ldct =
				bucket->bw_ldct + required_time_for_io;
		} else {
			/* Resuming BW from IDLE */
			bucket->bw_ldct = current_time;
		}

		--bucket->num_ios_waiting;
		io_entry->callback(io_entry->args);
		bucket->io_waitlist_qos_bc = io_entry->next;
		gsh_free(io_entry);
		io_entry = bucket->io_waitlist_qos_bc;
	}
	LogFullDebug(COMPONENT_QOS,
		     "<<<< SI:%d s_wio:%d op_type:%d lct:%ld sb_io:%d ",
		     share->share_id, share->num_ios_waiting, op_type,
		     bucket->bw_ldct, bucket->num_ios_waiting);
	pthread_mutex_unlock(&(bucket->lock));
}

/* since the IO in the queue are already consumed
 * and is suppose to schedule new IO */
static inline void pspc_rescedule_io_to_share(qos_bucket_t *sbucket,
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
		 * else indicate share limit has been reached
		 * so client is trottling */
		if ((cbucket->bw_ldct + required_time_for_io +
		     BW_SHARE_FW_IO_SCHEDULE) > current_time) {
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

static inline void print_io_details_ps(qos_share_t *share,
				       qos_bucket_t *sbucket,
				       unsigned int op_type,
				       uint64_t current_time, const char *str)
{
	if (share != NULL && sbucket != NULL)
		return;

	LogFullDebug(COMPONENT_QOS,
		     "%s:%d s_wio:%d op:%s pct:%ld sb_io:%d sbw_ldct:%ld", str,
		     share->share_id, share->num_ios_waiting,
		     (op_type == QOS_READ) ? "QOS_READ" : "QOS_WRITE",
		     current_time, sbucket->num_ios_waiting, sbucket->bw_ldct);
	list_timer_entries(sbucket->io_waitlist_qos_bc);
}

static inline void print_io_details_pc(qos_client_t *client,
				       qos_bucket_t *cbucket,
				       unsigned int op_type,
				       uint64_t current_time, const char *str)
{
	if (client != NULL && cbucket != NULL)
		return;

	LogFullDebug(COMPONENT_QOS,
		     "%s:%p c_wio:%d op:%s pct:%ld cb_io:%d cbw_ldct:%ld", str,
		     client->client_addr, client->num_ios_waiting,
		     (op_type == QOS_READ) ? "QOS_READ" : "QOS_WRITE",
		     current_time, cbucket->num_ios_waiting, cbucket->bw_ldct);
	list_timer_entries(cbucket->io_waitlist_qos_bc);
}

static inline void print_io_details_pspc(qos_share_t *share,
					 qos_client_t *client,
					 qos_bucket_t *sbucket,
					 qos_bucket_t *cbucket,
					 unsigned int op_type,
					 uint64_t current_time, const char *str)
{
	if (share == NULL || client == NULL || sbucket == NULL ||
	    cbucket == NULL)
		return;

	LogFullDebug(COMPONENT_QOS,
		     "%s SI:%d CI:%p s_wio:%d c_wio:%d op:%s lct:%ld pct:%ld",
		     str, share->share_id, client->client_addr,
		     share->num_ios_waiting, client->num_ios_waiting,
		     (op_type == QOS_READ) ? "QOS_READ" : "QOS_WRITE",
		     get_time_in_usec(), current_time);
	LogFullDebug(COMPONENT_QOS,
		     "sb_io:%d cb_io:%d sbw_ldct:%ld cbw_ldct:%ld",
		     sbucket->num_ios_waiting, cbucket->num_ios_waiting,
		     sbucket->bw_ldct, cbucket->bw_ldct);
	list_timer_entries(cbucket->io_waitlist_qos_bc);
	list_timer_entries(sbucket->io_waitlist_qos_bc);
}

static inline void print_all_io_details(void *qos_class, unsigned int op_type,
					unsigned int qos_class_type,
					const char *str)
{
	if (qos_class == NULL)
		return;

	if (qos_class_type == QOS_SHARE || qos_class_type == QOS_PSPC) {
		qos_share_t *share = qos_class;
		qos_bucket_t *sbucket =
			qos_get_bw_bucket(share, QOS_SHARE, op_type);

		LogDebug(COMPONENT_QOS, "got the qos share ######:%d",
			 share->share_id);
		print_io_details_ps(share, sbucket, op_type, 0, str);
		if (qos_class_type != QOS_SHARE) {
			qos_client_t *client = share->clients;

			while (client != NULL) {
				qos_bucket_t *cbucket = qos_get_bw_bucket(
					client, QOS_CLIENT, op_type);
				print_io_details_pspc(share, client, sbucket,
						      cbucket, op_type, 0, str);
				client = client->next;
			}
		}
	} else {
		qos_client_t *client = qos_class;
		qos_bucket_t *cbucket =
			qos_get_bw_bucket(client, QOS_CLIENT, op_type);

		LogDebug(COMPONENT_QOS, "got the qos client ######:%p",
			 client->client_addr);
		print_io_details_pc(client, cbucket, op_type, 0, str);
	}
}

static inline void pspc_reschedule_bw_io(qos_share_t *share,
					 unsigned int op_type)
{
	if (share == NULL)
		return;

	uint64_t current_time = get_time_in_usec();
	qos_client_t *client = share->clients;
	qos_bucket_t *sbucket = qos_get_bw_bucket(share, QOS_PSPC, op_type);

	if (sbucket == NULL)
		return;
	if (current_time > sbucket->bw_ldct) {
		pthread_mutex_lock(&sbucket->lock);
		while (client != NULL) {
			qos_bucket_t *cbucket =
				qos_get_bw_bucket(client, QOS_CLIENT, op_type);
			if (cbucket == NULL) {
				goto next;
				return;
			}
			pthread_mutex_lock(&cbucket->lock);
			if (current_time >= cbucket->bw_ldct &&
			    cbucket->num_ios_waiting != 0) {
				pspc_rescedule_io_to_share(sbucket, cbucket,
							   current_time);
			}
			pthread_mutex_unlock(&cbucket->lock);
next:
			client = client->next;
		}
		pthread_mutex_unlock(&sbucket->lock);
	}
}

bool ps_bw_control_cb(struct gsh_export *export, void *state)
{
	qos_share_t *share = get_share_qos(export);

	if (share && share->bw_enabled) {
		print_all_io_details(share, *(unsigned int *)state, QOS_CLIENT,
				     __func__);
		resume_bw_io_ps(share, *(unsigned int *)state);
	}
	/* Continue iteration */
	return true;
}
bool pc_bw_control_cb(struct gsh_client *cl, void *state)
{
	qos_client_t *client = get_client_qos(cl);

	if (client && client->bw_enabled) {
		print_all_io_details(client, *(unsigned int *)state, QOS_SHARE,
				     __func__);
		resume_bw_io_pc(client, *(unsigned int *)state);
	}
	/* Continue iteration */
	return true;
}
bool pspc_bw_control_cb(struct gsh_export *export, void *state)
{
	qos_share_t *share = get_share_qos(export);

	if (share && share->bw_enabled) {
		print_all_io_details(share, *(unsigned int *)state, QOS_PSPC,
				     __func__);
		pspc_reschedule_bw_io(share, *(unsigned int *)state);
		resume_bw_io_pspc(share, *(unsigned int *)state);
	}
	/* Continue iteration */
	return true;
}

static inline void resume_bw_io(unsigned int op_type)
{
	switch (g_qos_config->qos_type) {
	case QOS_NOT_ENABLED:
		LogFullDebug(COMPONENT_QOS, "QOS not enabled :%d",
			     g_qos_config->qos_type);
		break;
	case QOS_PS_ENABLED:
		foreach_gsh_export(ps_bw_control_cb, false, &op_type);
		break;
	case QOS_PC_ENABLED:
		foreach_gsh_client(pc_bw_control_cb, &op_type);
		break;
	case QOS_PS_PC_ENABLED:
		foreach_gsh_export(pspc_bw_control_cb, false, &op_type);
		break;
	default:
		LogFullDebug(COMPONENT_QOS, " Something really wrong:%d",
			     g_qos_config->qos_type);
		break;
	}
}

static void *qos_thread_func(void *arg)
{
	pthread_t current_thread_id = pthread_self();
	int counter = 0;
	unsigned int op_type = *(unsigned int *)arg;

	LogDebug(COMPONENT_QOS, "running from arg:%d thread.id:%lu ", op_type,
		 current_thread_id);
	while (true) {
		resume_bw_io(op_type);
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
		usleep(BW_DELAY_USEC);
		counter++;
	}
	return NULL;
}

pthread_t qos_thread[2] = { 0, 0 };
int var[2] = { QOS_READ, QOS_WRITE };
static void qos_thread_init(void)
{
	if (g_qos_config->enable_qos == 0)
		return;

	pthread_mutex_lock(&g_qos_lock);
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
	pthread_mutex_unlock(&g_qos_lock);
}

qos_client_t *get_client_qos(struct gsh_client *client)
{
	qos_client_t *qos_class = NULL;

	if (!client) {
		LogDebug(COMPONENT_QOS, " client is NULL");
		return NULL;
	}

	if (client->qos_class != NULL) {
		qos_class = client->qos_class;
	} else {
		LogDebug(COMPONENT_QOS, " qos_block is null and hoststr is :%s",
			 client->hostaddr_str);
	}

	return qos_class;
}

qos_share_t *get_share_qos(struct gsh_export *export)
{
	qos_share_t *qos_class = NULL;

	if (!export) {
		LogDebug(COMPONENT_QOS, "gsh_export is NULL");
		return NULL;
	}
	if (export->qos_block != NULL) {
		qos_class = export->qos_class;
		LogFullDebug(COMPONENT_QOS,
			     "Config update debugdp export:%p: share%p", export,
			     export->qos_class);
		if (qos_class == NULL) {
			LogDebug(COMPONENT_QOS, "qos_block is NULL path:%s",
				 export->cfg_fullpath);
			return NULL;
		}
	} else {
		LogDebug(COMPONENT_QOS, "qos_block is NULL path:%s",
			 export->cfg_fullpath);
		return NULL;
	}

	return qos_class;
}

uint32_t get_share_client_count(qos_share_t *s_qos_class)
{
	uint32_t count = 0;

	if (s_qos_class == NULL)
		return count;

	qos_client_t *c_qos_class = s_qos_class->clients;

	while (c_qos_class) {
		count++;
		c_qos_class = c_qos_class->next;
	}

	return count;
}
