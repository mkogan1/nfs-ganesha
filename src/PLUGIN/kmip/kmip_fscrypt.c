// SPDX-License-Identifier: LGPL-3.0-or-later
/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright IBM Corporation, 2010
 * Author: Marcus Watts <mwatts@ibm.com>
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
 * 02110-1301 USA
 *
 * -------------
 */

/* main.c
 * Module core functions
 */

#include <stdlib.h>
#include <assert.h>
#include "gsh_list.h"
#include "log.h"
#include "FSAL/fsal_init.h"
#include "abstract_mem.h"
#include "config_parsing.h"
#include "conf_url.h"
#include "nfs_exports.h"
#include "kmip.h"
#include "kmip_bio.h"
#include "kmip_memset.h"

struct kmip_host_param {
	struct glist_head link;
	char *name;
	unsigned short port;
};
struct kmip_params {
	char *kmip_cert;
	char *kmip_key;
	char *kmip_ca;
	char *kmip_user;
	char *kmip_password;
	struct glist_head kmip_host;
};

struct export_kmip {
	char *kmip_key_id;
	uint16_t export_id;
};

struct kmip_params kmip_settings;

void *kmip_host_init(void *, void *);
int kmip_host_commit(void *, void *, void *, struct config_error_type *);
int kmip_load_export_extension(struct export_extension *,
		config_file_t, struct config_error_type *);
int kmip_root_cb_func(struct exp_root_callback *,
	struct fsal_obj_handle *obj);
int kmip_root_cb_free(struct exp_root_callback *);

// XXX how to do more than one host?
static struct config_item kmip_host_params[] = {
	CONF_ITEM_STR("addr", 0, 512, "localhost.", kmip_host_param,
			  name),
	CONF_ITEM_UI16("port", 1, UINT16_MAX, 5696,
		       kmip_host_param, port), /* default is kmip */
	CONFIG_EOL
};

static struct config_item kmip_params[] = {
	CONF_ITEM_PATH("cert", 1, MAXPATHLEN, NULL, kmip_params,
		kmip_cert),
	CONF_ITEM_PATH("key", 1, MAXPATHLEN, NULL, kmip_params,
		kmip_key),
	CONF_ITEM_PATH("ca", 1, MAXPATHLEN, NULL, kmip_params,
		kmip_ca),
	CONF_ITEM_STR("user", 0, 512, NULL, kmip_params,
		kmip_user),
	CONF_ITEM_STR("password", 0, 512, NULL, kmip_params,
		kmip_password),

	CONF_ITEM_BLOCK_MULT("HOST", kmip_host_params, kmip_host_init,
			kmip_host_commit, kmip_params,
			kmip_host),
	CONFIG_EOL
};

struct config_block kmip_block = {
	.dbus_interface_name = "org.ganesha.nfsd.config.fscrypt.kmip",
	.blk_desc.name = "KMIP",
	.blk_desc.type = CONFIG_BLOCK,
	.blk_desc.flags = CONFIG_UNIQUE,
	.blk_desc.u.blk.init = noop_conf_init,
	.blk_desc.u.blk.params = kmip_params,
	.blk_desc.u.blk.commit = noop_conf_commit
};

static struct config_item kmip_export_params[] = {
	CONF_ITEM_STR("kmip_key_id", 0, 512, NULL, export_kmip,
		kmip_key_id),
	CONF_MAND_UI16("Export_id", 0, UINT16_MAX, 1, export_kmip, export_id),    \

	CONFIG_EOL
};

int kmip_export_extension_commit(void *, void *, void *, struct config_error_type*);

struct config_block kmip_export_extensions = {
	.dbus_interface_name = "org.ganesha.nfsd.config.kmip.%d",
	.blk_desc.name = "EXPORT",
	.blk_desc.flags = CONFIG_RELAX,
	.blk_desc.type = CONFIG_BLOCK,
	.blk_desc.u.blk.init = noop_conf_init,
	.blk_desc.u.blk.params = kmip_export_params,
	.blk_desc.u.blk.commit = kmip_export_extension_commit
};

struct export_extension_sw kmip_extension_sw = {
	kmip_load_export_extension
};

struct kmip_export_extension {
	struct export_extension extension;
} kmip_export_extension_st = {
	.extension = {
		.sw = &kmip_extension_sw
	}
};

struct exp_root_callback_sw kmip_root_callback_sw = {
	kmip_root_cb_func,
	kmip_root_cb_free
};

struct kmip_callback {
	struct exp_root_callback callback;
	char *kmip_key_id;
};

void free_host_params()
{
	struct glist_head *host_list = &kmip_settings.kmip_host;
	struct kmip_host_param *host_p;
	if (glist_null(host_list)) {
		return;
	}
        while ((host_p = glist_first_entry(host_list,
			struct kmip_host_param, link))) {
                glist_del(&host_p->link);
		gsh_free(host_p);
        }
}

void *kmip_host_init(void *link_mem, void *self_struct)
{
	assert (link_mem || self_struct);
	if (!link_mem) {
		return self_struct;
	}
	if (!self_struct) {
		struct kmip_host_param *host_p;
		host_p = gsh_calloc(1, sizeof *host_p);
		return host_p;
	} else {
		gsh_free(self_struct);
	}
	return 0;
}

int kmip_host_commit(void *node, void *link_mem, void *self_struct,
	struct config_error_type * err_type)
{
	struct glist_head *host_list = link_mem;
	struct kmip_host_param *host_p = self_struct;
	if (glist_null(host_list)) {
		glist_init(host_list);
	}
	glist_add_tail(host_list, &host_p->link);
	return 0;
}

int kmip_init_block(config_file_t config_struct,
				 struct config_error_type *err_type)
{
	int rc;

	rc = load_config_from_parse(config_struct, &kmip_block,
				     &kmip_settings, true,
				     err_type);

	if (glist_null(&kmip_settings.kmip_host)) {
		glist_init(&kmip_settings.kmip_host);
	}

	/*
	 * All kmip options are optional, so no kmip block
	 * is not necessarily bad.
	 */
	if (!config_error_is_harmless(err_type))
		LogDebug(COMPONENT_FSAL, "Parsing kmip block failed");
	if (rc > 0)
		rc = 0;
	return rc;
}

int load_kmip_export_extensions(config_file_t in_config,
				struct config_error_type *err_type)
{
	int rc;
	struct export_kmip st[1];
	memset(st, 0, sizeof *st);
	rc = load_config_from_parse(in_config,
		&kmip_export_extensions, st, false, err_type);

	return rc;
}

// XXX kill this...
void dummy_routine_to_prove_i_can_link_to_libkmip()
{
KMIP *a = 0;
BIO *b = 0;
char *c = 0;
int d = 0;
char **e = 0;
int *f = 0;
int g;
g = kmip_bio_send_request_encoding(a,b,c,d,e,f);
printf ("g = %d\n", g);
}

struct kmip_plugin_module {
	struct gsh_config_provider config;
};

struct kmip_plugin_module kmip_plugin_static_t = {
	.config = {
		.init_block = kmip_init_block,
	}
};

/**
 * @brief Associate kmip_key_id with export
 */

int kmip_export_extension_commit(void *node, void *link_mem, void *self_struct,
	struct config_error_type *err_type)
{
	struct kmip_callback *cb;
	struct export_kmip *st = self_struct;
	struct gsh_export *exp;
	int err_count = 0;
	exp = get_gsh_export(st->export_id);
	if (!exp) {
		 LogCrit(COMPONENT_CONFIG, "Export %d does not exist",
                         st->export_id);
		return ++err_count;
	}
	cb = gsh_calloc(1, sizeof *cb);
	cb->kmip_key_id = gsh_strdup(st->kmip_key_id);
	add_to_export_callbacks(exp, &kmip_root_callback_sw, &cb->callback);
	put_gsh_export_config(exp);
	return 0;
}

int kmip_load_export_extension(struct export_extension *ex,
config_file_t in_config, struct config_error_type *err_type)
{
	int rc;
	struct export_kmip st[1];
 __attribute__((unused))        // don't need for now; optimizer will delete
	struct kmip_export_extension *extension_st;
	extension_st = container_of(ex, struct kmip_export_extension, extension);
	memset(st, 0, sizeof *st);
	rc = load_config_from_parse(in_config,
		&kmip_export_extensions, st, false, err_type);
	return rc;
}

/**
 * @brief Initialize kmip plugin
 */

MODULE_INIT void init(void)
{
	LogDebug(COMPONENT_FSAL, "kmip load");
	if (register_config_locked(&kmip_plugin_static_t.config) != 0) {
		LogCrit(COMPONENT_FSAL, "Failed to register kmip plugin.");
	}
	add_export_extension(&kmip_export_extension_st.extension);
}

/**
 * @brief Release kmip plugin
 */

MODULE_FINI void finish(void)
{
	LogDebug(COMPONENT_FSAL, "kmip unload");

	remove_export_extension(&kmip_export_extension_st.extension);
	free_host_params();
	if (unregister_config_locked(&kmip_plugin_static_t.config) != 0)
		fprintf(stderr, "KMIP module failed to unregister");
}

int kmip_root_cb_func(struct exp_root_callback *cb,
	struct fsal_obj_handle *obj)
{
	struct kmip_callback *data = container_of(cb, struct kmip_callback, callback);
	struct gsh_export *export = cb->export;
	fsal_status_t status;
	int rc = 0;
	char *cp;	// XXX temp kill
	unsigned char *up, *tp, *ep;	// XXX temp kill

struct {
	uint64_t data[8];
} dummy_key = {
.data = {
0x7d9a63c09eefd3aa,
0x416e43558f09444a,
0xfa6b8492fb432604,
0x9942c6f001df5b31,
0xf22c42b11fc3657b,
0x6eb0f9fa5603c7d2,
0x515db02cab0333f3,
0xbb4142bc42ed8f6d
} };

	if (!data->kmip_key_id) {
		LogCrit(COMPONENT_FSAL, "keyset callback: export = %d, obj = %p; no kmip_key_id",
			export->export_id, obj);
		return 0;
	}

	// XXX KMIP CALL GOES HERE

	up = (unsigned char *) (dummy_key.data);	// XXX temp kill
	ep = up + sizeof dummy_key.data;	// XXX temp kill
	tp = up;	// XXX temp kill
	for (cp = data->kmip_key_id; *cp; ++cp) {	// XXX temp kill
		*tp ^= *cp;	// XXX temp kill
		++tp;	// XXX temp kill
		if (tp >= ep) tp = up;	// XXX temp kill
	}	// XXX temp kill

	LogCrit(COMPONENT_FSAL, "keyset callback: kmip_key_id = %s, export = %d, obj = %p",
		data->kmip_key_id, export->export_id, obj);
	status = obj->obj_ops->control(obj, FSCRYPT_SETKEY, &dummy_key);

	if (!FSAL_IS_SUCCESS(status)) {
		LogCrit(COMPONENT_FSAL, "keyset failed: kmip_key_id = %s, export = %d, error = %d/%d",
			data->kmip_key_id, export->export_id, status.major, status.minor);
		rc = EINVAL;
	}

	kmip_root_cb_free(cb);
	return rc;
}

int kmip_root_cb_free(struct exp_root_callback *cb)
{
	struct kmip_callback *data = container_of(cb, struct kmip_callback, callback);

	gsh_free(data->kmip_key_id);
	gsh_free(data);
	return 0;
}
