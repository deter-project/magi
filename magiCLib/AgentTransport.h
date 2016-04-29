#ifndef _AGENT_TRANSPORT_H
#define _AGENT_TRANSPORT_H

#include "AgentRequest.h"
#include <pthread.h>

typedef struct AgentRequestQueueNode {
	AgentRequest_t* req;
	struct AgentRequestQueueNode* next;
} AgentRequestQueueNode_t;

typedef struct AgentRequestQueue {
	AgentRequestQueueNode_t* front;
	AgentRequestQueueNode_t* rear;
	pthread_mutex_t qlock;
} AgentRequestQueue_t;

void init_connection(char* commHost, int commPort);
void start_connection(char* dockName, char* commGroup);
void closeTransport();

void enqueue(AgentRequestQueue_t *transport, AgentRequest_t *req);
AgentRequest_t* dequeue(AgentRequestQueue_t *transport);
void sendOut(AgentRequest_t* req);

#endif /* _AGENT_TRANSPORT_H */

