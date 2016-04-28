#ifndef _MAGIMESSAGE_H
#define _MAGIMESSAGE_H

#include "Util.h"

#include <stdint.h>

typedef enum {
	NONE,
	BLOB,
	TEXT,
	IMAGE,
	PROTOBUF,
	YAML,
	XML,
	PICKLE,
	Internal_ForceMyEnumIntSize1 = sizeof(uint8_t)
} contentType_t;

typedef enum {
	ISACK = 1,
	NOAGG = 2,
	WANTACK = 4,
	Internal_ForceMyEnumIntSize2 = sizeof(uint8_t)
} msgType_t;

typedef enum {
	SEQUENCE = 1,
	TIMESTAMP = 2,
	SEQUENCEID = 3,
	HOSTTIME = 4,
	SRC = 20,
	SRCDOCK = 21,
	HMAC = 22,
	DSTNODES = 50,
	DSTGROUPS = 51,
	DSTDOCKS = 52,
	Internal_ForceMyEnumIntSize3 = sizeof(uint8_t)
} options_t;

typedef struct header {
	options_t type;
	uint8_t len;
	char* value;
	struct header* next;
} headerOpt_t;

typedef struct MAGImessage {
	uint32_t id;
	msgType_t flags;
	contentType_t contentType;
	char* data;
	list_t *dstGroups;
	list_t *dstNodes;
	list_t *dstDocks;
	char* src;
	char* srcDock;
	headerOpt_t* headers;
} MAGIMessage_t;

typedef struct fargs {
	char* func;
	char* args[10];
	char* trigger;
} fargs_t;

MAGIMessage_t* createMagiMessage(char* srcdock, char* node, char* group,
		char* dstdock, contentType_t contenttype, char* data);
char* encodeMagiMessage(MAGIMessage_t* magiMsg, uint32_t* bufLen);
MAGIMessage_t* decodeMagiMessage(char* msgBuf);
fargs_t decodeMsgDataYaml(char* yamlEncodedData);

void freeMagiMessage(MAGIMessage_t* msg);

#endif /* _MAGIMESSAGE_H */
