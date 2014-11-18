#include <stdint.h>
typedef struct list_s {
	char* name;
	struct list_s* next;
}list_t;

typedef enum{
	NONE,
	BLOB,
	TEXT,
	IMAGE,
	PROTOBUF,
	YAML,
	XML,
	PICKLE,
	Internal_ForceMyEnumIntSize1 = sizeof(uint8_t)
}contentType_t;


typedef enum {
	ISACK = 1,
	NOAGG = 2,
	WANTACK = 4,
	Internal_ForceMyEnumIntSize2 = sizeof(uint8_t)
}msgType_t;


typedef enum{
	SEQUENCE = 1,
	TIMESTAMP = 2,
	SEQUENCEID = 3,
	HOSTTIME = 4,
	SRC = 20,
	SRCDOCK = 21,
	HMAC = 22,
	DSTNODES =50,
	DSTGROUPS = 51,
	DSTDOCKS = 52,
	Internal_ForceMyEnumIntSize3 = sizeof(uint8_t)
}options_t;


typedef struct header{
	options_t type;
	uint8_t len;
	char * value;
	struct header* next;
}headerOpt_t;

typedef struct fargs{
	char* func;
	char* args[10];
}fargs_t;

typedef struct MAGImessage{
	uint32_t length;
	uint16_t headerLength;
	uint32_t id;
	list_t* dstGroups,*dstNodes,*dstDocks; 
	msgType_t flags;
	contentType_t contentType;
	headerOpt_t* headers;   
	char* data;
	fargs_t funcArgs;
}MAGIMessage_t;


