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
 * @file nfs_qosmgr.c
 * @brief Routines used for managing the QOS via DBUS.
 *	-> Run time updation of BW etc.
 *
 *
 */
#include "nfs_core.h"
#include "nfs_qos.h"
#include "nfs_qosmgr.h"

/*  QoS Method Arguments */
#define CLIENT_IP_ARG { "client_ip", "s", "in" }
#define READ_BW_IN_ARG { "read_bw", "t", "in" }
#define WRITE_BW_IN_ARG { "write_bw", "t", "in" }
#define READ_BW_OUT_ARG { "read_bw", "t", "out" }
#define WRITE_BW_OUT_ARG { "write_bw", "t", "out" }
#define SUCCESS_ARG { "success", "b", "out" }
#define SHARE_ID_ARG { "id", "q", "in" }
#define MAX_TOKENS_IN { "max_tokens", "u", "in" }
#define MAX_TOKENS_OUT { "max_tokens", "u", "out" }
#define TOKEN_RENEW_IN { "token_renewal", "t", "in" }
#define TOKEN_RENEW_OUT { "token_renewal", "t", "out" }
#define CLIENT_LIST_ARG { "client_list", "a(sttuu)", "out" }
#define TOTAL_CLIENTS { "total_clients", "u", "out" }
#define OFFSET_ARG { "offset", "u", "in" }
#define LIMIT_ARG { "limit", "u", "in" }
#define QOS_CLIENT_CONTAINER "(s(ss)(ss)(ss))"
#define QOS_CLIENTS_REPLY { "clients", "a(s(ss)(ss)(ss))", "out" }

struct showclients_state {
	DBusMessageIter client_iter;
};

#define CHECK_DBUS_NEXT_ARG_OR_RETURN(args, type, iter, msg)        \
	do {                                                        \
		if (!dbus_message_iter_next(args) ||                \
		    dbus_message_iter_get_arg_type(args) != type) { \
			gsh_dbus_status_reply(&iter, false, msg);   \
			return true;                                \
		}                                                   \
	} while (0)

#define CHECK_DBUS_ARG_OR_RETURN(args, type, iter, msg)                      \
	do {                                                                 \
		if (!args || dbus_message_iter_get_arg_type(args) != type) { \
			gsh_dbus_status_reply(&iter, false, msg);            \
			return true;                                         \
		}                                                            \
	} while (0)

#define CHECK_ARG_AND_RETURN(args, iter, msg)                     \
	do {                                                      \
		if (!args) {                                      \
			gsh_dbus_status_reply(&iter, false, msg); \
			return true;                              \
		}                                                 \
	} while (0)

static bool dbus_qos_client_bw_set(DBusMessageIter *args, DBusMessage *reply,
				   DBusError *error)
{
	struct gsh_client *client_ip;
	uint64_t read_bw, write_bw;
	struct QoS_perClient_Class *client_qos;
	DBusMessageIter iter;
	char *errormsg = "OK";

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_STRING, iter,
				 "Invalid arg ClientIP");
	client_ip = lookup_client(args, &errormsg);
	CHECK_ARG_AND_RETURN(client_ip, iter, "Client IP address not found");

	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg read_bw");
	dbus_message_iter_get_basic(args, &read_bw);
	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg write_bw");
	dbus_message_iter_get_basic(args, &write_bw);

	client_qos = get_client_qos(&client_ip->cl_addrbuf);
	if (client_qos) {
		PTHREAD_MUTEX_lock(&client_qos->lock);
		client_qos->read_bucket.max_bw_allowed = read_bw;
		client_qos->write_bucket.max_bw_allowed = write_bw;
		PTHREAD_MUTEX_unlock(&client_qos->lock);
		return true;
	}
	return false;
}

/*  Token Control Methods */
static bool dbus_qos_client_token_set(DBusMessageIter *args, DBusMessage *reply,
				      DBusError *error)
{
	struct gsh_client *client_ip;
	uint64_t max_tokens;
	uint64_t token_renewal;
	struct QoS_perClient_Class *client_qos;
	DBusMessageIter iter;
	char *errormsg = "OK";

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_STRING, iter,
				 "Invalid arg ClientIP");
	client_ip = lookup_client(args, &errormsg);
	CHECK_ARG_AND_RETURN(client_ip, iter, errormsg);

	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg max_token");
	dbus_message_iter_get_basic(args, &max_tokens);
	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg token_renewal");
	dbus_message_iter_get_basic(args, &token_renewal);

	client_qos = get_client_qos(&client_ip->cl_addrbuf);
	if (client_qos) {
		PTHREAD_MUTEX_lock(&client_qos->lock);
		client_qos->read_bucket.max_available_tokens = max_tokens;
		client_qos->write_bucket.max_available_tokens = max_tokens;
		client_qos->read_bucket.tokens_renew_time = token_renewal;
		client_qos->write_bucket.tokens_renew_time = token_renewal;
		PTHREAD_MUTEX_unlock(&client_qos->lock);
		return true;
	}
	return false;
}

static bool dbus_qos_client_token_get(DBusMessageIter *args, DBusMessage *reply,
				      DBusError *error)
{
	struct gsh_client *client_ip;
	struct QoS_perClient_Class *client_qos;
	DBusMessageIter iter;
	char *errormsg = "OK";

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_STRING, iter,
				 "Invalid arg ClientIP");
	client_ip = lookup_client(args, &errormsg);
	CHECK_ARG_AND_RETURN(client_ip, iter, errormsg);

	client_qos = get_client_qos(&client_ip->cl_addrbuf);
	if (client_qos) {
		PTHREAD_MUTEX_lock(&client_qos->lock);
		uint32_t max_tokens =
			client_qos->read_bucket.max_available_tokens;
		uint64_t token_renewal =
			client_qos->read_bucket.tokens_renew_time;

		PTHREAD_MUTEX_unlock(&client_qos->lock);

		dbus_message_iter_append_basic(args, DBUS_TYPE_UINT32,
					       &max_tokens);
		dbus_message_iter_append_basic(args, DBUS_TYPE_UINT64,
					       &token_renewal);
		return true;
	}
	return false;
}
static bool dbus_qos_client_bw_get(DBusMessageIter *args, DBusMessage *reply,
				   DBusError *error)
{
	struct gsh_client *client_ip;
	struct QoS_perClient_Class *client_qos;
	DBusMessageIter iter;
	char *errormsg = "OK";

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_STRING, iter,
				 "Invalid arg ClientIP");
	client_ip = lookup_client(args, &errormsg);
	CHECK_ARG_AND_RETURN(client_ip, iter, errormsg);

	client_qos = get_client_qos(&client_ip->cl_addrbuf);
	if (client_qos) {
		PTHREAD_MUTEX_lock(&client_qos->lock);
		uint64_t read_bw = client_qos->read_bucket.max_bw_allowed;
		uint64_t write_bw = client_qos->write_bucket.max_bw_allowed;

		PTHREAD_MUTEX_unlock(&client_qos->lock);

		dbus_message_iter_append_basic(args, DBUS_TYPE_UINT64,
					       &read_bw);
		dbus_message_iter_append_basic(args, DBUS_TYPE_UINT64,
					       &write_bw);
		return true;
	}
	return false;
}

static bool client_qos_to_dbus(qos_client_t *client, void *state)
{
	struct showclients_state *iter_state = state;
	DBusMessageIter client_struct, bw_struct;
	char ipaddr[INET6_ADDRSTRLEN];
	const char *ip_str = ipaddr;
	char read_bw_str[32], write_bw_str[32], enable_bw_str[32];
	const char *read_bw_ptr, *write_bw_ptr, *enable_bw_ptr;
	const char *enable_bw_label = "BW_ENABLED";
	const char *read_label = "READ_BW";
	const char *write_label = "WRITE_BW";

	if (client->client_addr->ss_family == AF_INET) {
		struct sockaddr_in *sin =
			(struct sockaddr_in *)client->client_addr;
		inet_ntop(AF_INET, &sin->sin_addr, ipaddr, sizeof(ipaddr));
	} else {
		struct sockaddr_in6 *sin6 =
			(struct sockaddr_in6 *)client->client_addr;
		inet_ntop(AF_INET6, &sin6->sin6_addr, ipaddr, sizeof(ipaddr));
	}

	dbus_message_iter_open_container(&iter_state->client_iter,
					 DBUS_TYPE_STRUCT, NULL,
					 &client_struct);

	dbus_message_iter_append_basic(&client_struct, DBUS_TYPE_STRING,
				       &ip_str);

	snprintf(read_bw_str, sizeof(read_bw_str), "%lu",
		 client->read_bucket.max_bw_allowed);
	read_bw_ptr = read_bw_str;

	snprintf(write_bw_str, sizeof(write_bw_str), "%lu",
		 client->write_bucket.max_bw_allowed);
	write_bw_ptr = write_bw_str;

	snprintf(enable_bw_str, sizeof(enable_bw_str), "%d",
		 client->bw_enabled);
	enable_bw_ptr = enable_bw_str;

	dbus_message_iter_open_container(&client_struct, DBUS_TYPE_STRUCT, NULL,
					 &bw_struct);
	dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
				       &enable_bw_label);
	dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
				       &enable_bw_ptr);
	dbus_message_iter_close_container(&client_struct, &bw_struct);

	dbus_message_iter_open_container(&client_struct, DBUS_TYPE_STRUCT, NULL,
					 &bw_struct);
	dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
				       &read_label);
	dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
				       &read_bw_ptr);
	dbus_message_iter_close_container(&client_struct, &bw_struct);

	dbus_message_iter_open_container(&client_struct, DBUS_TYPE_STRUCT, NULL,
					 &bw_struct);
	dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
				       &write_label);
	dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
				       &write_bw_ptr);
	dbus_message_iter_close_container(&client_struct, &bw_struct);

	dbus_message_iter_close_container(&iter_state->client_iter,
					  &client_struct);

	return true;
}

static bool dbus_qos_pspc_clients_bw_list(DBusMessageIter *args,
					  DBusMessage *reply, DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;
	struct showclients_state iter_state;
	uint32_t total_clients = 0;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (!share) {
		put_gsh_export(export);
		return false;
	}

	dbus_message_iter_init_append(reply, &iter);

	PTHREAD_MUTEX_lock(&share->lock);
	total_clients = get_share_client_count(share);
	PTHREAD_MUTEX_unlock(&share->lock);

	dbus_message_iter_append_basic(&iter, DBUS_TYPE_UINT32, &total_clients);

	dbus_message_iter_open_container(&iter, DBUS_TYPE_ARRAY,
					 QOS_CLIENT_CONTAINER,
					 &iter_state.client_iter);

	qos_client_t *client = share->clients;

	while (client) {
		client_qos_to_dbus(client, &iter_state);
		client = client->next;
	}

	dbus_message_iter_close_container(&iter, &iter_state.client_iter);
	put_gsh_export(export);
	return true;
}
static bool dbus_qos_share_bw_get(DBusMessageIter *args, DBusMessage *reply,
				  DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter, bw_struct;
	char read_bw_str[32], write_bw_str[32], enable_bw_str[32];
	const char *read_bw_ptr, *write_bw_ptr, *enable_bw_ptr;
	const char *read_label = "READ_BW";
	const char *write_label = "WRITE_BW";
	const char *bw_label = "ENABLE_BW";

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (share) {
		PTHREAD_MUTEX_lock(&share->lock);

		dbus_message_iter_open_container(&iter, DBUS_TYPE_STRUCT, NULL,
						 &bw_struct);

		snprintf(enable_bw_str, sizeof(enable_bw_str), "%d",
			 share->bw_enabled);
		enable_bw_ptr = enable_bw_str;
		snprintf(read_bw_str, sizeof(read_bw_str), "%lu",
			 share->read_bucket.max_bw_allowed);
		read_bw_ptr = read_bw_str;
		snprintf(write_bw_str, sizeof(write_bw_str), "%lu",
			 share->write_bucket.max_bw_allowed);
		write_bw_ptr = write_bw_str;

		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &bw_label);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &enable_bw_ptr);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &read_label);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &read_bw_ptr);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &write_label);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &write_bw_ptr);

		dbus_message_iter_close_container(&iter, &bw_struct);
		PTHREAD_MUTEX_unlock(&share->lock);
	}

	put_gsh_export(export);
	return true;
}

static bool dbus_qos_share_token_get(DBusMessageIter *args, DBusMessage *reply,
				     DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter, token_struct;
	char read_token_str[32], write_token_str[32];
	const char *read_token_ptr, *write_token_ptr;
	const char *read_label = "READ_TOKENS";
	const char *write_label = "WRITE_TOKENS";

	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (share) {
		PTHREAD_MUTEX_lock(&share->lock);

		dbus_message_iter_init_append(reply, &iter);
		dbus_message_iter_open_container(&iter, DBUS_TYPE_STRUCT, NULL,
						 &token_struct);

		snprintf(read_token_str, sizeof(read_token_str), "%lu",
			 share->read_bucket.max_available_tokens);
		read_token_ptr = read_token_str;
		snprintf(write_token_str, sizeof(write_token_str), "%lu",
			 share->write_bucket.max_available_tokens);
		write_token_ptr = write_token_str;

		dbus_message_iter_append_basic(&token_struct, DBUS_TYPE_STRING,
					       &read_label);
		dbus_message_iter_append_basic(&token_struct, DBUS_TYPE_STRING,
					       &read_token_ptr);
		dbus_message_iter_append_basic(&token_struct, DBUS_TYPE_STRING,
					       &write_label);
		dbus_message_iter_append_basic(&token_struct, DBUS_TYPE_STRING,
					       &write_token_ptr);

		dbus_message_iter_close_container(&iter, &token_struct);
		PTHREAD_MUTEX_unlock(&share->lock);
	}

	put_gsh_export(export);
	return true;
}

static bool dbus_qos_share_default_client_bw_get(DBusMessageIter *args,
						 DBusMessage *reply,
						 DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter, bw_struct;
	char read_bw_str[32], write_bw_str[32];
	const char *read_bw_ptr, *write_bw_ptr;
	const char *read_label = "MAX_CLIENT_READ_BW";
	const char *write_label = "MAX_CLIENT_WRITE_BW";

	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	if (!export)
		return false;

	share = get_share_qos(export);
	if (share) {
		PTHREAD_MUTEX_lock(&share->lock);

		dbus_message_iter_init_append(reply, &iter);
		dbus_message_iter_open_container(&iter, DBUS_TYPE_STRUCT, NULL,
						 &bw_struct);

		snprintf(read_bw_str, sizeof(read_bw_str), "%lu",
			 export->qos_block->max_client_read_bw);
		read_bw_ptr = read_bw_str;
		snprintf(write_bw_str, sizeof(write_bw_str), "%lu",
			 export->qos_block->max_client_write_bw);
		write_bw_ptr = write_bw_str;

		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &read_label);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &read_bw_ptr);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &write_label);
		dbus_message_iter_append_basic(&bw_struct, DBUS_TYPE_STRING,
					       &write_bw_ptr);

		dbus_message_iter_close_container(&iter, &bw_struct);
		PTHREAD_MUTEX_unlock(&share->lock);
	}

	put_gsh_export(export);
	return true;
}
static bool dbus_qos_share_bw_set(DBusMessageIter *args, DBusMessage *reply,
				  DBusError *error)
{
	uint16_t export_id;
	uint64_t read_bw, write_bw;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg read_bw");
	dbus_message_iter_get_basic(args, &read_bw);
	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg write_bw");
	dbus_message_iter_get_basic(args, &write_bw);

	share = get_share_qos(export);
	if (share) {
		PTHREAD_MUTEX_lock(&share->lock);
		share->read_bucket.max_bw_allowed = read_bw;
		share->write_bucket.max_bw_allowed = write_bw;
		PTHREAD_MUTEX_unlock(&share->lock);
	}
	put_gsh_export(export);
	return true;
}

static bool dbus_qos_share_token_set(DBusMessageIter *args, DBusMessage *reply,
				     DBusError *error)
{
	uint16_t export_id;
	uint64_t max_tokens;
	uint64_t token_renewal;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg max_token");
	dbus_message_iter_get_basic(args, &max_tokens);
	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg token_renewal");
	dbus_message_iter_get_basic(args, &token_renewal);

	share = get_share_qos(export);
	if (share) {
		PTHREAD_MUTEX_lock(&share->lock);
		share->read_bucket.max_available_tokens = max_tokens;
		share->write_bucket.max_available_tokens = max_tokens;
		share->read_bucket.tokens_renew_time = token_renewal;
		share->write_bucket.tokens_renew_time = token_renewal;
		PTHREAD_MUTEX_unlock(&share->lock);
	}
	put_gsh_export(export);
	return true;
}

static bool dbus_qos_pspc_clients_bw_set(DBusMessageIter *args,
					 DBusMessage *reply, DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	qos_client_t *client;
	struct gsh_client *client_ip;
	uint64_t read_bw, write_bw;
	DBusMessageIter iter;
	char *errormsg = "OK";

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (!share) {
		put_gsh_export(export);
		return false;
	}
	dbus_message_iter_next(args);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_STRING, iter,
				 "Invalid arg ClientIP");
	client_ip = lookup_client(args, &errormsg);
	CHECK_ARG_AND_RETURN(client_ip, iter, errormsg);

	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg read_bw");
	dbus_message_iter_get_basic(args, &read_bw);
	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg write_bw");
	dbus_message_iter_get_basic(args, &write_bw);

	client = pspc_get_client_from_list(share->clients,
					   &client_ip->cl_addrbuf);
	if (client) {
		PTHREAD_MUTEX_lock(&client->lock);
		client->bw_enabled = true;
		client->read_bucket.max_bw_allowed = read_bw;
		client->write_bucket.max_bw_allowed = write_bw;
		PTHREAD_MUTEX_unlock(&client->lock);
		return true;
	}

	put_gsh_export(export);
	return true;
}

static bool dbus_qos_share_default_client_bw_set(DBusMessageIter *args,
						 DBusMessage *reply,
						 DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	uint64_t read_bw, write_bw;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (!share) {
		put_gsh_export(export);
		return false;
	}

	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg read_bw");
	dbus_message_iter_get_basic(args, &read_bw);
	CHECK_DBUS_NEXT_ARG_OR_RETURN(args, DBUS_TYPE_UINT64, iter,
				      "Invalid arg write_bw");
	dbus_message_iter_get_basic(args, &write_bw);

	export->qos_block->max_client_read_bw = read_bw;
	export->qos_block->max_client_write_bw = write_bw;
	put_gsh_export(export);

	return true;
}

static bool dbus_qos_enable_bw_control_ps(DBusMessageIter *args,
					  DBusMessage *reply, DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (g_qos_config->enable_qos && g_qos_config->enable_bw_control &&
	    share) {
		QoS_perShareInsert(export, NULL);
	} else {
		put_gsh_export(export);
		gsh_dbus_status_reply(&iter, false, "check global values");
		return true;
	}

	put_gsh_export(export);

	return true;
}

static bool dbus_qos_disable_bw_control_ps(DBusMessageIter *args,
					   DBusMessage *reply, DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (share) {
		pthread_mutex_lock(&share->lock);
		qos_drain_bw_ios(share, QOS_SHARE);
		pthread_mutex_unlock(&share->lock);
	} else {
		put_gsh_export(export);
		gsh_dbus_status_reply(&iter, false, "check global values");
		return true;
	}

	put_gsh_export(export);

	return true;
}

static bool dbus_qos_disable_bw_control_pspc(DBusMessageIter *args,
					     DBusMessage *reply,
					     DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (share) {
		pthread_mutex_lock(&share->lock);
		qos_client_t *client = share->clients;

		while (client != NULL) {
			qos_drain_bw_ios(client, QOS_CLIENT);
			client = client->next;
		}
		qos_drain_bw_ios(share, QOS_SHARE);
		pthread_mutex_unlock(&share->lock);
	} else {
		put_gsh_export(export);
		gsh_dbus_status_reply(&iter, false, "check global values");
		return true;
	}

	put_gsh_export(export);

	return true;
}

static bool dbus_qos_enable_bw_control_pspc(DBusMessageIter *args,
					    DBusMessage *reply,
					    DBusError *error)
{
	uint16_t export_id;
	struct gsh_export *export;
	qos_share_t *share;
	DBusMessageIter iter;

	dbus_message_iter_init_append(reply, &iter);
	CHECK_DBUS_ARG_OR_RETURN(args, DBUS_TYPE_UINT16, iter,
				 "Invalid arg exportid");
	dbus_message_iter_get_basic(args, &export_id);
	export = get_gsh_export(export_id);
	CHECK_ARG_AND_RETURN(export, iter, "Export id not found");

	share = get_share_qos(export);
	if (share) {
		pthread_mutex_lock(&share->lock);
		qos_client_t *client = share->clients;

		while (client != NULL) {
			client->bw_enabled = 1;
			client = client->next;
		}
		share->bw_enabled = 1;
		pthread_mutex_unlock(&share->lock);
	} else {
		put_gsh_export(export);
		gsh_dbus_status_reply(&iter, false, "check global values");
		return true;
	}

	put_gsh_export(export);

	return true;
}
/* PerShare-PerClient implemnentation */

static struct gsh_dbus_method qos_share_clients_list = {
	.name = "ListShareClientsBandwidth",
	.method = dbus_qos_pspc_clients_bw_list,
	.args = { SHARE_ID_ARG, TOTAL_CLIENTS, QOS_CLIENTS_REPLY, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_pspc_clients_bw_set = {
	.name = "SetShareClientBandwidth",
	.method = dbus_qos_pspc_clients_bw_set,
	.args = { SHARE_ID_ARG, IPADDR_ARG, READ_BW_IN_ARG, WRITE_BW_IN_ARG,
		  SUCCESS_ARG, END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_default_client_bw_get = {
	.name = "GetShareDefaultClientBandwidth",
	.method = dbus_qos_share_default_client_bw_get,
	.args = { SHARE_ID_ARG,
		  { "default_client_bandwidth", "a(ss)", "out" },
		  SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_default_client_bw_set = {
	.name = "SetShareDefaultClientBandwidth",
	.method = dbus_qos_share_default_client_bw_set,
	.args = { SHARE_ID_ARG, READ_BW_IN_ARG, WRITE_BW_IN_ARG, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_enable_all_clients_bw_control_pspc = {
	.name = "EnableAllClientQosBwControlPSPC",
	.method = dbus_qos_enable_bw_control_pspc,
	.args = { SHARE_ID_ARG, SUCCESS_ARG, END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_disable_all_clients_bw_control_pspc = {
	.name = "DisableShareQosBwControlPSPC",
	.method = dbus_qos_disable_bw_control_pspc,
	.args = { SHARE_ID_ARG, SUCCESS_ARG, END_ARG_LIST }
};

/* Per Share ibut is common for PerShare-PerClient also*/
static struct gsh_dbus_method qos_share_bw_get = {
	.name = "GetShareBandwidth",
	.method = dbus_qos_share_bw_get,
	.args = { SHARE_ID_ARG,
		  { "bandwidth", "a(sss)", "out" },
		  SUCCESS_ARG,
		  END_ARG_LIST }
};
static struct gsh_dbus_method qos_share_bw_set = {
	.name = "SetShareBandwidth",
	.method = dbus_qos_share_bw_set,
	.args = { SHARE_ID_ARG, READ_BW_IN_ARG, WRITE_BW_IN_ARG, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_token_get = {
	.name = "GetShareTokens",
	.method = dbus_qos_share_token_get,
	.args = { SHARE_ID_ARG,
		  { "tokens", "a(ss)", "out" },
		  SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_token_set = {
	.name = "SetShareTokens",
	.method = dbus_qos_share_token_set,
	.args = { SHARE_ID_ARG, MAX_TOKENS_IN, TOKEN_RENEW_IN, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method *qos_methods_pspc[] = {
	&qos_share_bw_get,
	&qos_share_bw_set,
	&qos_share_token_get,
	&qos_share_token_set,
	&qos_share_clients_list,
	&qos_pspc_clients_bw_set,
	&qos_share_default_client_bw_get,
	&qos_share_default_client_bw_set,
	&qos_share_enable_all_clients_bw_control_pspc,
	&qos_share_disable_all_clients_bw_control_pspc,
	NULL
};

static struct gsh_dbus_method qos_share_enable_bw_control = {
	.name = "EnableShareQosBwControl",
	.method = dbus_qos_enable_bw_control_ps,
	.args = { SHARE_ID_ARG, SUCCESS_ARG, END_ARG_LIST }
};

static struct gsh_dbus_method qos_share_disable_bw_control = {
	.name = "DisableShareQosBwControl",
	.method = dbus_qos_disable_bw_control_ps,
	.args = { SHARE_ID_ARG, SUCCESS_ARG, END_ARG_LIST }
};
static struct gsh_dbus_method *qos_methods_ps[] = {
	&qos_share_bw_get,
	&qos_share_bw_set,
	&qos_share_token_get,
	&qos_share_token_set,
	&qos_share_enable_bw_control,
	&qos_share_disable_bw_control,
	NULL
};

/* PerClient implemnentation */
static struct gsh_dbus_method qos_client_bw_set = {
	.name = "SetClientBandwidth",
	.method = dbus_qos_client_bw_set,
	.args = { CLIENT_IP_ARG, READ_BW_IN_ARG, WRITE_BW_IN_ARG, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_client_bw_get = {
	.name = "GetClientBandwidth",
	.method = dbus_qos_client_bw_get,
	.args = { CLIENT_IP_ARG, READ_BW_OUT_ARG, WRITE_BW_OUT_ARG, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_client_token_set = {
	.name = "SetClientTokens",
	.method = dbus_qos_client_token_set,
	.args = { CLIENT_IP_ARG, MAX_TOKENS_IN, TOKEN_RENEW_IN, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method qos_client_token_get = {
	.name = "GetClientTokens",
	.method = dbus_qos_client_token_get,
	.args = { CLIENT_IP_ARG, MAX_TOKENS_OUT, TOKEN_RENEW_OUT, SUCCESS_ARG,
		  END_ARG_LIST }
};

static struct gsh_dbus_method *qos_methods_pc[] = {
	&qos_client_bw_get, &qos_client_bw_set, &qos_client_token_get,
	&qos_client_token_set, NULL
};

static struct gsh_dbus_interface qos_interface = {
	.name = "org.ganesha.nfsd.qos",
	.props = NULL,
	.methods = NULL,
	.signals = NULL
};

static struct gsh_dbus_interface *dbus_qos_interface[] = { &qos_interface,
							   NULL };

/*  Initialize QoS manager */
void dbus_qosmgr_init(void)
{
	if (g_qos_config->enable_qos) {
		switch (g_qos_config->qos_type) {
		case QOS_PS_ENABLED:
			qos_interface.methods = qos_methods_ps;
			break;
		case QOS_PC_ENABLED:
			qos_interface.methods = qos_methods_pc;
			break;
		case QOS_PS_PC_ENABLED:
			qos_interface.methods = qos_methods_pspc;
			break;
		}
		gsh_dbus_register_path("QosMgr", dbus_qos_interface);
	}
}
