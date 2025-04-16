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

/**
 * @brief Initialize kmip plugin
 *
 * To do: register config options.
 */

MODULE_INIT void init(void)
{
	LogDebug(COMPONENT_FSAL, "kmip load");
}

/**
 * @brief Release kmip plugin
 *
 * To do: de-register config options.
 */

MODULE_FINI void finish(void)
{
	LogDebug(COMPONENT_CONFIG, "kmip unload");
}
