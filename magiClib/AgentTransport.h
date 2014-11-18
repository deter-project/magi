#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include "AgentRequest.h"
#include "MAGIMessage.h"
#include "logger.h"


typedef struct Queue{	
	AgentRequest_t* req; 
	struct Queue* next;
}Queue_t;


typedef struct Transport{

	Queue_t *front;
	Queue_t *rear;
	Logger* logger;
	/*int loglevel;*/
	pthread_mutex_t qlock;

}Transport_t;

void sendOut(AgentRequest_t* req);
int isEmpty(Transport_t* transport);
AgentRequest_t* dequeue(Transport_t *transport);
Transport_t * enqueue(Transport_t *transport,AgentRequest_t *req);

