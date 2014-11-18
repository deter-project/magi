#include <stdint.h>

typedef enum {
	ACK =1,
	SOURCE_ORDERING,
	TIME_STAMP,
Internal_ForceMyEnumIntSize5 = sizeof(uint8_t)
}AgentOptions_t;

typedef enum {
	JOIN_GROUP = 1,
	LEAVE_GROUP = 2,
	LISTEN_DOCK = 3,
	UNLISTEN_DOCK = 4,
	MESSAGE = 5,
	Internal_ForceMyEnumIntSize = sizeof(uint8_t)	
}AgentRequestType_t;

/*
 * Options field in the AgentRequest Message
 * */
typedef struct AgentRequestOptions
{
	AgentOptions_t options; /*type : Check */
	uint32_t len; 
	char* value; /*TODO: Value int or type unknown?*/
	struct AgentRequestOptions * next;
}AgentRequestOptions_t; 

/*
 *This structure holds the AgentRequest Message
 */
typedef struct AgentRequest
{
	AgentRequestType_t reqType;
	AgentRequestOptions_t* options;
	char* data;
}AgentRequest_t;


/*
 *Functions
 */
char * AgentEncode(AgentRequest_t* msg,uint32_t* bufLen );
AgentRequest_t*  AgentDecode(char* buf);
