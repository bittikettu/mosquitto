#ifndef DB_DUMP_H
#define DB_DUMP_H
/*
Copyright (c) 2010-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#include <persist.h>

void print__client(struct P_client *chunk, uint32_t length);
void print__client_msg(struct P_client_msg *chunk, uint32_t length);
void print__base_msg(struct P_base_msg *chunk, uint32_t length);
void print__sub(struct P_sub *chunk, uint32_t length);
void dumpjsonarray(dbid_t seed);
void readindexcache(void);

extern char GLB_exportfolder[100];

#endif
