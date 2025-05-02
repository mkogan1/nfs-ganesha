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

struct kmip_params kmip_settings;

void *kmip_host_init(void *, void *);
int kmip_host_commit(void *, void *, void *, struct config_error_type *);

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

int init_block(config_file_t config_struct,
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

struct kmip_plugin_module {
	struct gsh_config_provider config;
};

struct kmip_plugin_module kmip_plugin_static_t = {
	.config = {
		.init_block = init_block,
	}
};

/**
 * @brief Initialize kmip plugin
 *
 * To do: register config options.
 */

MODULE_INIT void init(void)
{
	LogDebug(COMPONENT_FSAL, "kmip load");
	if (register_config_locked(&kmip_plugin_static_t.config) != 0) {
		LogCrit(COMPONENT_FSAL, "Failed to register kmip plugin.");
	}
}

/**
 * @brief Release kmip plugin
 *
 * To do: de-register config options.
 */

MODULE_FINI void finish(void)
{
	LogDebug(COMPONENT_FSAL, "kmip unload");

	free_host_params();
	if (unregister_config_locked(&kmip_plugin_static_t.config) != 0)
		fprintf(stderr, "KMIP module failed to unregister");
}
