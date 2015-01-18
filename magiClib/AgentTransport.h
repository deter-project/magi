#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include "logger.h"
#include "AgentRequest.h"

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
