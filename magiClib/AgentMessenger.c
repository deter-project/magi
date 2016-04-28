#include "AgentMessenger.h"
#include "AgentTransport.h"
#include "AgentRequest.h"
#include "Logger.h"

#include <string.h>

// Extern data structure from AgentTransport files.
extern AgentRequestQueue_t *rxQueue, *txQueue;

// Global agent logger
extern Logger* logger;

/**
 * Return the next message in the receive queue
 */
MAGIMessage_t* next(int block) {
	AgentRequest_t* req;
	if (block) {
		//replace spin by notify
		while ((req = dequeue(rxQueue)) == NULL) {
			//block
		}
	} else {
		req = dequeue(rxQueue);
	}

	if (req == NULL)
		return NULL;

	MAGIMessage_t* magiMsg = NULL;

	if (req->reqType == MESSAGE) {
		log_info(logger, "Agent Request with MAGI message received");
		magiMsg = decodeMagiMessage(req->data);
	} else {
		log_info(logger, "Agent Request with Non-MAGI message received");
	}

	freeAgentRequest(req);

	return magiMsg;
}

/**
 * Send out a message
 */
void sendMsg(MAGIMessage_t* magiMsg, char* arg, ...) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	//Encoded magi message length
	uint32_t encodedMsgLength = 0;
	/*MAGI header and data is encoded*/
	char* encodedMsg = encodeMagiMessage(magiMsg, &encodedMsgLength);

	log_debug(logger, "Encoded Magi Message Length: %d", encodedMsgLength);

	AgentRequest_t* req = createAgentRequest(MESSAGE, encodedMsg,
			encodedMsgLength);
	free(encodedMsg);

	if (arg != NULL) {
		va_list kw;
		va_start(kw, arg);
		char* args = arg;
		char* key, *value, *tok;
		char* str;

		do {
			str = (char*) malloc(strlen(args));
			strncpy(str, args, strlen(args));
			tok = strtok(str, "=");
			key = (char*) malloc(strlen(tok));
			strncpy(key, tok, strlen(tok));
			if (key == NULL) {
				log_error(logger, "Invalid option");
				free(key);
				continue;
			}
			tok = strtok(NULL, "=");
			value = (char*) malloc(strlen(tok));
			strncpy(value, tok, strlen(tok));

			if (value == NULL) {
				log_error(logger, "Invalid option");
				free(key);
				free(value);
				continue;
			}

			add_options(&req->options, key, strlen(value), value);
			free(str);
			free(key);

		} while ((args = va_arg(kw, char*)) != NULL);
		va_end(kw);
	}
	sendOut(req);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Create a trigger and send it as a MAGI Message
 *
 * @param group if not null, group is added to the list of destination groups
 * @param dock if not null, dock is add to the list of destination docks
 * @param contenttype the contenttype for the data as specified in the Messenger interface
 * @param data encoded data for the message or null for none.
 */
void trigger(char* groups, char* docks, contentType_t contenttype, char* data) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	log_info(logger, "Sending out trigger: %s", data);
	MAGIMessage_t* msg = createMagiMessage(NULL, NULL, groups, docks,
			contenttype, data);
	sendMsg(msg, NULL);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Would like to see messages for group
 */
void joinGroup(char* group) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	AgentRequest_t* req = createAgentRequest(JOIN_GROUP, group,
			(uint32_t) strlen(group));
	sendOut(req);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * No longer care about messages for group
 */
void leaveGroup(char* group) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	AgentRequest_t* req = createAgentRequest(LEAVE_GROUP, group,
			(uint32_t) strlen(group));
	sendOut(req);
	exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Start listening for messages destined for 'dock'
 */
void listenDock(char* dock) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	AgentRequest_t* req = createAgentRequest(LISTEN_DOCK, dock,
			(uint32_t) strlen(dock));
	exitlog(logger, __func__, __FILE__, __LINE__);
	sendOut(req);
}

/**
 * Stop listening for messages destined for 'dock'
 */
void unlistenDock(char* dock) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	AgentRequest_t* req = createAgentRequest(UNLISTEN_DOCK, dock,
			(uint32_t) strlen(dock));
	sendOut(req);
	exitlog(logger, __func__, __FILE__, __LINE__);
}
