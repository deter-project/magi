#include "AgentRequest.h"
#include "logger.h"
typedef struct queue{

	AgentRequest_t* msg_p;
  	struct queue *next;
}queue_t;	



