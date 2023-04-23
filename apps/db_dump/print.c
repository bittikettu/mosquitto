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

#include <inttypes.h>
#include <stdio.h>

#include "db_dump.h"
#include <mosquitto_broker_internal.h>
#include <memory_mosq.h>
#include <mqtt_protocol.h>
#include <persist.h>
#include <property_mosq.h>
#include <jansson.h>
#include <zlib.h>
#include <sys/stat.h>

json_t *dataarray = 0;
dbid_t indexi = -1;

int gzstore(const char *istream, const char *fname) {
	gzFile file;
	int err;

	file = gzopen(fname, "wb");
	if (file == NULL) {
		fprintf(stderr, "gzopen error\n");
		return(1);
	}

	if (gzputs(file, istream) == -1) {
		fprintf(stderr, "gzputs err: %s\n", gzerror(file, &err));
		return(1);
	}
	gzseek(file, 1L, SEEK_CUR); /* add one zero byte */
	gzclose(file);
	return 0;
}

static void print__properties(mosquitto_property *properties)
{
	int i;

	if(properties == NULL) return;

	printf("\tProperties:\n");

	while(properties){
		switch(properties->identifier){
			/* Only properties for base messages are valid for saving */
			case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
				printf("\t\tPayload format indicator: %d\n", properties->value.i8);
				break;

			case MQTT_PROP_CONTENT_TYPE:
				printf("\t\tContent type: %s\n", properties->value.s.v);
				break;

			case MQTT_PROP_RESPONSE_TOPIC:
				printf("\t\tResponse topic: %s\n", properties->value.s.v);
				break;

			case MQTT_PROP_CORRELATION_DATA:
				printf("\t\tCorrelation data: ");
				for(i=0; i<properties->value.bin.len; i++){
					printf("%02X", properties->value.bin.v[i]);
				}
				printf("\n");
				break;

			case MQTT_PROP_USER_PROPERTY:
				printf("\t\tUser property: %s , %s\n", properties->name.v, properties->value.s.v);
				break;

			default:
				printf("\t\tInvalid property type: %d\n", properties->identifier);
				break;
		}

		properties = properties->next;
	}
}


void print__client(struct P_client *chunk, uint32_t length)
{
	printf("DB_CHUNK_CLIENT:\n");
	printf("\tLength: %d\n", length);
	printf("\tClient ID: %s\n", chunk->clientid);
	if(chunk->username){
		printf("\tUsername: %s\n", chunk->username);
	}
	if(chunk->F.listener_port > 0){
		printf("\tListener port: %u\n", chunk->F.listener_port);
	}
	printf("\tLast MID: %d\n", chunk->F.last_mid);
	printf("\tSession expiry time: %" PRIu64 "\n", chunk->F.session_expiry_time);
	printf("\tSession expiry interval: %u\n", chunk->F.session_expiry_interval);
}


void print__client_msg(struct P_client_msg *chunk, uint32_t length)
{
	printf("DB_CHUNK_CLIENT_MSG:\n");
	printf("\tLength: %d\n", length);
	printf("\tClient ID: %s\n", chunk->clientid);
	printf("\tStore ID: %" PRIu64 "\n", chunk->F.store_id);
	printf("\tMID: %d\n", chunk->F.mid);
	printf("\tQoS: %d\n", chunk->F.qos);
	printf("\tRetain: %d\n", (chunk->F.retain_dup&0xF0)>>4);
	printf("\tDirection: %d\n", chunk->F.direction);
	printf("\tState: %d\n", chunk->F.state);
	printf("\tDup: %d\n", chunk->F.retain_dup&0x0F);
	if(chunk->subscription_identifier){
		printf("\tSubscription identifier: %d\n", chunk->subscription_identifier);
	}
}

void dumpjsonarray(dbid_t seed) {
	extern long base_msg_count;
	char filename[50] = { 0 };
	char *ret_strings = NULL;
	int chars = 0;
	ret_strings = json_dumps(dataarray, JSON_COMPACT | JSON_PRESERVE_ORDER | JSON_REAL_PRECISION(2));
//	chars = printf("%s\n",ret_strings);
	chars = sprintf(filename, "./%s/", GLB_exportfolder, base_msg_count / 10000);
	if (access(filename, F_OK) != 0) {
		mkdir(filename, 0755);
	}

	chars = sprintf(filename, "./%s/%ld/", GLB_exportfolder, base_msg_count / 10000);
	if (access(filename, F_OK) != 0) {
		mkdir(filename, 0755);
		printf("----- %s\n",filename);
	}

	memset(filename, 0x00, 50);
	chars = sprintf(filename, "./conversion2/%ld/%lld", base_msg_count / 10000, seed);

	gzstore(ret_strings, filename);
	memset(ret_strings, 0x00, chars);
	free(ret_strings);
	json_array_clear(dataarray);
}

void readindexcache(void) {
	FILE *fp;

	fp = fopen("./number.bin", "rb");
	if(fp != 0){
		fread(&indexi,1, sizeof(dbid_t),fp);
		fclose(fp);
		printf("indeksi %ld\n",indexi);
		sleep(4);
	}
}

void print__base_msg(struct P_base_msg *chunk, uint32_t length)
{
	uint8_t *payload;
	json_t *root = NULL;
	static int counter = 0;
	FILE *fp;
//	printf("DB_CHUNK_BASE_MSG:\n");
//	printf("\tLength: %d\n", length);
//	printf("\tStore ID: %" PRIu64 "\n", chunk->F.store_id);
	/* printf("\tSource ID: %s\n", chunk->source_id); */
	/* printf("\tSource Username: %s\n", chunk->source_username); */
//	printf("\tSource Port: %d\n", chunk->F.source_port);
//	printf("\tSource MID: %d\n", chunk->F.source_mid);
//	printf("\tTopic: %s\n", chunk->topic);
//	printf("\tQoS: %d\n", chunk->F.qos);
//	printf("\tRetain: %d\n", chunk->F.retain);
//	printf("\tPayload Length: %d\n", chunk->F.payloadlen);
//	printf("\tExpiry Time: %" PRIu64 "\n", chunk->F.expiry_time);

	payload = chunk->payload;
		/* Print payloads with UTF-8 data below an arbitrary limit of 256 bytes */
	if (payload != 0 && chunk->F.payloadlen > 3) {
		if (mosquitto_validate_utf8((char*) payload, (uint16_t) chunk->F.payloadlen) == MOSQ_ERR_SUCCESS) {
			if (dataarray == 0) {
				dataarray = json_array();
			}
			if (chunk->F.store_id < indexi) {
				json_t *data = 0;
				data = json_loads(payload, 0, 0);
				json_t *arrayvalue = 0;
				size_t indexi;
				json_array_foreach(data, indexi, arrayvalue)
				{
					json_array_append(dataarray, arrayvalue);
					counter++;
				}
				if ((counter > 400) != 0) {
					counter = 0;
					fp = fopen("./number.bin.tmp", "wb");
					if (fp != 0) {
						fwrite(&chunk->F.store_id, 1, sizeof(dbid_t), fp);
						fclose(fp);
						sync();
						rename("./number.bin.tmp", "./number.bin");
					}
					dumpjsonarray(chunk->F.store_id);
				}
			} else {
				printf("SKIP %ld %ld\n", indexi, chunk->F.store_id);
			}
		}
	}

	print__properties(chunk->properties);
}


void print__sub(struct P_sub *chunk, uint32_t length)
{
	printf("DB_CHUNK_SUB:\n");
	printf("\tLength: %u\n", length);
	printf("\tClient ID: %s\n", chunk->clientid);
	printf("\tTopic: %s\n", chunk->topic);
	printf("\tQoS: %d\n", chunk->F.qos);
	printf("\tSubscription ID: %d\n", chunk->F.identifier);
	printf("\tOptions: 0x%02X\n", chunk->F.options);
}


