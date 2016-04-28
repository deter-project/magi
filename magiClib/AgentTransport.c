#include "AgentTransport.h"
#include "AgentMessenger.h"
#include "MAGIMessage.h"
#include "Logger.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>

AgentRequestQueue_t* rxQueue, *txQueue;

pthread_t sender, listener;

static int listener_stop = 0, listener_clear = 1;
static int fd;

struct sockaddr_in serv_addr;/*Holds Address info of server*/

extern char* preamble;
extern Logger* logger;

/*
 * Enqueing request on a given transport
 */
void enqueue(AgentRequestQueue_t *transport, AgentRequest_t *req) {
	/*Add new nodes at rear*/
	pthread_mutex_lock(&(transport->qlock));

	log_debug(logger, "Enqueing request");
	log_debug(logger, "Request Type: %d", req->reqType);

	if (transport->rear == NULL) {
		log_debug(logger, "Queue NULL, Enqueing request");
		transport->rear = (AgentRequestQueueNode_t *) malloc(
				sizeof(AgentRequestQueueNode_t));
		transport->rear->next = NULL;
		transport->rear->req = req;
		//transport->rear->req->data = req->data;
		transport->front = transport->rear;
	} else {
		log_debug(logger, "Queue not NULL; Enqueing request at rear");
		AgentRequestQueueNode_t* newNode = (AgentRequestQueueNode_t *) malloc(
				sizeof(AgentRequestQueueNode_t));
		transport->rear->next = newNode;
		newNode->req = req;
		newNode->next = NULL;
		transport->rear = newNode;
	}
	pthread_mutex_unlock(&(transport->qlock));
}

/*
 * Dequeing request from a given transport
 */
AgentRequest_t* dequeue(AgentRequestQueue_t *transport) {
	pthread_mutex_lock(&(transport->qlock));
	AgentRequestQueueNode_t* front = transport->front;
	if (front == NULL) {
		/*No elements in  the queue*/
		//log_debug(logger, "Dequeue: No elements in the queue");
		pthread_mutex_unlock(&(transport->qlock));
		return NULL;
	} else {
		log_debug(logger, "Dequeue: Got an element on the queue");
		AgentRequest_t* req = front->req;
		transport->front = front->next;
		if (transport->front == NULL) {
			transport->rear = NULL;
		}
		log_debug(logger, "Request Type: %d", req->reqType);
		free(front);
		pthread_mutex_unlock(&(transport->qlock));
		return req;
	}
}

void sendOut(AgentRequest_t* req) {
	log_info(logger, "Enqueue Agent Request on txQueue");
	enqueue(txQueue, req);
}

/**
 * Send thread to write outgoing messages
 */
void* sendThd() {
	entrylog(logger, __func__, __FILE__, __LINE__);
	AgentRequest_t* req = NULL;
	while (1) {
		req = dequeue(txQueue);
		if (req != NULL) {
			log_info(logger, "Message found on txQueue. Sending it out.");
			uint32_t length = 0;
			char* encodedRequest = encodeAgentRequest(req, &length);

			log_debug(logger, "Sending out msg of size %d on socket", length);
			int bytesSent = 0;
			bytesSent = write(fd, encodedRequest, length);
			log_debug(logger, "Sent out %d bytes on socket", bytesSent);

			if (bytesSent == -1) {
				log_error(logger, "Message send failed");
			}

			free(encodedRequest);
			freeAgentRequest(req);
			req = NULL;
		}
		nanosleep((const struct timespec[]){{0, 100000000L}}, NULL);
	}
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Listening thread to read incoming messages
 */
void* listenThd() {
	entrylog(logger, __func__, __FILE__, __LINE__);

	char msgBuf[1024];
	char tempBuf[1024];
	char firstEight[8];

	while (1) {
		/*pthread_Cancel should exit from read blocking call. If a bug appears, then
		 read has to be timed*/

		int bytesRead = read(fd, msgBuf, sizeof(msgBuf));
		if (bytesRead < 8)
			continue;

		memcpy(firstEight, msgBuf, 8);
		if (strncmp(firstEight, preamble, 8)) {
			log_error(logger,
					"Invalid Agent Request. Does not begin with preamble");
			continue;
		}

		log_info(logger, "Received a agent request");
		char* ptr = msgBuf;
		ptr = ptr + 8;

		uint32_t totalLen;
		memcpy(&totalLen, ptr, 4);
		totalLen = ntohl(totalLen);

		log_debug(logger, "Agent Request Total Length: %d", totalLen);

		int totalBytesRead = bytesRead;
		while (totalBytesRead < totalLen) {
			bytesRead = read(fd, tempBuf, (totalLen - totalBytesRead));
			if (bytesRead <= 0)
				continue;
			memcpy(msgBuf + totalBytesRead, tempBuf, bytesRead);
			totalBytesRead += bytesRead;
		}

		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
		listener_clear = 0;

		log_info(logger, "Decoding Agent Request");
		AgentRequest_t* req = decodeAgentRequest(msgBuf);

		log_info(logger, "Enqueue Agent Request on rxQueue");
		enqueue(rxQueue, req);

		listener_clear = 1;
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
		pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
		/*Notify next() if non-blocking*/

		nanosleep((const struct timespec[]){{0, 100000000L}}, NULL);
	}

	exitlog(logger, __func__, __FILE__, __LINE__);
}

void init_connection(char* commHost, int commPort) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	/*Set up transport queues*/
	rxQueue = (AgentRequestQueue_t*) malloc(sizeof(AgentRequestQueue_t));
	txQueue = (AgentRequestQueue_t*) malloc(sizeof(AgentRequestQueue_t));
	pthread_mutex_init(&rxQueue->qlock, NULL);
	pthread_mutex_init(&txQueue->qlock, NULL);
	rxQueue->front = rxQueue->rear = NULL;
	txQueue->front = txQueue->rear = NULL;

	/*init socket*/
	fd = socket(PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		perror("Socket creation failed: ");
		exit(0);
	}

	bzero(&serv_addr, sizeof(struct sockaddr_in));

	/*Set addr and port*/
	struct hostent* server = gethostbyname2(commHost, AF_INET);

	serv_addr.sin_family = server->h_addrtype;
	serv_addr.sin_port = htons(commPort);

	inet_aton(server->h_addr, &serv_addr.sin_addr);
	//serv_addr.sin_addr.s_addr=(inet_addr("127.0.0.1"));

	/*bcopy((char *) server->h_addr, (char *) &serv_addr.sin_addr.s_addr,
	 server->h_length);*/

	exitlog(logger, __func__, __FILE__, __LINE__);
}

void start_connection(char* dockName, char* commGroup) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	//Connect to the given socket
	if (connect(fd, (struct sockaddr*) &serv_addr, sizeof(serv_addr))) {
		log_error(logger, "Connection to daemon failed: ");
		exit(1);
	}
	log_info(logger, "Agent connected to daemon");

	/*Start Sender Thd*/
	int err = pthread_create(&sender, NULL, sendThd, NULL);
	if (err < 0) {
		log_error(logger, "Error in creating sender");
		exit(0);
	}
	/*Start Listen Thd*/
	err = pthread_create(&listener, NULL, listenThd, NULL);
	if (err < 0) {
		log_error(logger, "Error in creating listener");
		exit(0);
	}
	log_info(logger, "Agent Sender and Listener threads active");

	log_info(logger, "Sending a listen dock request for dock %s", dockName);
	listenDock(dockName);

	if (commGroup != NULL) {
		log_info(logger, "Sending a join group request for group %s",
				commGroup);
		joinGroup(commGroup);
	}

	exitlog(logger, __func__, __FILE__, __LINE__);
}

void closeTransport() {
	entrylog(logger, __func__, __FILE__, __LINE__);

	log_info(logger, "Shutting down listener thread");
	pthread_cancel(listener);
	listener_stop = 1;

	log_info(logger, "Waiting for txQueue to empty out");
	while (txQueue->front != NULL) {
		sleep(1);
	}
	sleep(1);

	//ready to close send thd and close the socket
	log_info(logger, "Shutting down sender thread");
	pthread_cancel(sender);
	close(fd);

	free(rxQueue);
	free(txQueue);

	exitlog(logger, __func__, __FILE__, __LINE__);
}
