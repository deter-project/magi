#include "MAGIMessage.h"
#include "Logger.h"

#include <string.h>

extern char* hostName;
extern Logger* logger;

MAGIMessage_t* allocateInitializedMagiMessage();

int calMagiMsgHdrLen(headerOpt_t* headers) {
	//entrylog(logger, __func__, __FILE__, __LINE__);
	int len = 0;
	while (headers) {
		len += headers->len;
		len += 2;
		headers = headers->next;
	}
	//exitlog(logger, __func__, __FILE__, __LINE__);
	return len;
}

/****************************************************
 * Helper function to insert header into a list
 * ***************************************************/
void insert_header(MAGIMessage_t* msg, uint8_t type, uint8_t len, char* value) {
	//entrylog(logger, __func__, __FILE__, __LINE__);
	headerOpt_t * list = msg->headers;
	headerOpt_t * opt_header = (headerOpt_t *) malloc(sizeof(headerOpt_t));
	opt_header->type = type;
	opt_header->len = strlen(value);
	if (len == 0 || value == NULL) {
		log_error(logger,
				"illegal header in function: %s\n\t in File \"%s\", line %d \n",
				__func__, __FILE__, __LINE__);
	}
	opt_header->value = malloc(opt_header->len + 1);
	memcpy(opt_header->value, value, strlen(value));
	opt_header->next = NULL;

	if (list == NULL) {
		list = opt_header;
	} else {
		headerOpt_t * tmp = list;
		while (tmp->next)
			tmp = tmp->next;
		tmp->next = opt_header;
	}

	if (type == DSTDOCKS)
		addList(&msg->dstDocks, value);
	else if (type == DSTGROUPS)
		addList(&msg->dstGroups, value);
	else if (type == DSTNODES)
		addList(&msg->dstNodes, value);
	else if (type == SRC){
		msg->src = malloc(strlen(value) + 1);
		strcpy(msg->src, value);
	} else if (type == SRCDOCK) {
		msg->srcDock = malloc(strlen(value) + 1);
		strcpy(msg->srcDock, value);
	}

	msg->headers = list;
	//exitlog(logger, __func__, __FILE__, __LINE__);
}

/**
 * Function: decodeMsgDataYaml
 * This function takes a YAML message as input and extracts the arguments,
 * method, trigger information if any.
 */
fargs_t decodeMsgDataYaml(char* yamlEncodedData) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	char* dataBuf = (char*) malloc(strlen(yamlEncodedData) + 1);
	strcpy(dataBuf, yamlEncodedData);

	log_info(logger, "YAML Encoded Data : %s", dataBuf);

	fargs_t funcInfo;
	funcInfo.func = NULL;
	funcInfo.args[0] = NULL;
	funcInfo.trigger = NULL;

	char*tkn;
	int charPosition = 0, lineLength = 0;

	int msgLength = strlen(dataBuf);
	for (charPosition = 0; charPosition < strlen(dataBuf); charPosition++) {
		if (dataBuf[charPosition] == '\'' || dataBuf[charPosition] == '\"')
			dataBuf[charPosition] = ' ';
	}

	nxt: while (dataBuf) {
		log_debug(logger, "Parsing next line");
		lineLength = 0;
		charPosition = 0;
		while (dataBuf[charPosition] != '\n') {
			lineLength++;
			charPosition++;
			if (charPosition > msgLength) {
				log_debug(logger, "Reached the end of message");
				return funcInfo;
			}
		}

		//read 1 line
		char* line = (char*) malloc(lineLength + 1);
		strncpy(line, dataBuf, lineLength);
		line[lineLength] = '\0';
		log_debug(logger, "Parsing line: %s", line);

		char * ck;
		if ((ck = strstr(line, "method")) != NULL) {
			log_debug(logger, "YAML: method");
			//This line has method
			tkn = strtok(line, ":");
			tkn = strtok(NULL, ":,");
			//func name
			log_debug(logger, "Function name : %s", tkn);
			tkn = trimwhitespace(tkn);
			funcInfo.func = (char*) malloc(strlen(tkn) + 1);
			sprintf(funcInfo.func, "%s", tkn);

		} else if ((ck = strstr(line, "args")) != NULL) {
			log_debug(logger, "YAML: args");
			//args: {key1: '1', key2: '2'}
			tkn = strtok(line, ":"); //key - args
			if (tkn == NULL) {
				dataBuf = dataBuf + lineLength + 1;
				goto nxt;
			}
			tkn = strtok(NULL, "}"); //val -> args list
			char* tstr = (char*) malloc(strlen(tkn) + 1);
			strcpy(tstr, tkn);
			char *t1 = strchr(tstr, '{');
			if (t1) {
				t1++;
				tstr = t1;
			} else {
				free(line);
				dataBuf = dataBuf + lineLength + 1;
				goto nxt;
			}
			// tstr = " key1: 1,key2:2 " or " key1:1 "
			char * t;
			t = strtok(tstr, ",:"); //key
			int argItr = 0;
			do {
				//1 key value pair
				t = strtok(NULL, ":,%"); //value
				if (t == NULL) {
					dataBuf = dataBuf + lineLength + 1;
					goto nxt;
				}
				t = trimwhitespace(t);
				log_debug(logger, "arg value[%d]: %s", argItr, t);
				t = trimwhitespace(t);
				char* temp = (char*) malloc(strlen(t));
				strncpy(temp, t, strlen(t) + 1);
				funcInfo.args[argItr] = temp;
				argItr++;
			} while ((t = strtok(NULL, ",:%")) != NULL); //key
			funcInfo.args[argItr] = NULL;
			log_debug(logger, "Parsed all args");
		} else if ((ck = strstr(line, "trigger")) != NULL) {
			log_debug(logger, "YAML: trigger");
			tkn = strtok(line, ":");
			tkn = strtok(NULL, ":,");
			//event name
			log_debug(logger, "Event name : %s", tkn);
			tkn = trimwhitespace(tkn);
			funcInfo.trigger = (char*) malloc(strlen(tkn) + 1);
			sprintf(funcInfo.trigger, "%s", tkn);
		}
		dataBuf = dataBuf + lineLength + 1;
	}

	free(dataBuf);

	exitlog(logger, __func__, __FILE__, __LINE__);
	return funcInfo;
}

/**************************************************************************************
 * Function: decodeMagiMessage
 * This function returns MAGIMessage structure from buffer (deserialize)
 * This function allocates memory for the structure. This should be freed by the caller.
 **************************************************************************************/
MAGIMessage_t* decodeMagiMessage(char* encodedMagiMessage) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	/*Caller has realized it is a MAGI message*/

	char* msgBufPtr = encodedMagiMessage;
	int len_optHeaders;

	//msgBufPtr points to total length
	uint32_t totalLength;
	memcpy(&totalLength, msgBufPtr, 4);
	totalLength = ntohl(totalLength);
	log_debug(logger, "Magi Message Total Length: %d", totalLength);

	msgBufPtr = msgBufPtr + 4; //header length
	uint16_t headerLength;
	memcpy(&headerLength, msgBufPtr, 2);
	headerLength = ntohs(headerLength);
	log_debug(logger, "Magi Message Header Length: %d", headerLength);

	len_optHeaders = headerLength - 6;

	MAGIMessage_t* magiMsg = allocateInitializedMagiMessage();
	msgBufPtr = msgBufPtr + 2; //message id
	memcpy(&magiMsg->id, msgBufPtr, 4);
	magiMsg->id = ntohl(magiMsg->id);

	msgBufPtr = msgBufPtr + 4; //flags
	char flags;
	memcpy(&flags, (char*) msgBufPtr, 1);
	magiMsg->flags = (uint8_t) flags;

	msgBufPtr = msgBufPtr + 1; //content type
	char contentType;
	memcpy(&contentType, (char*) msgBufPtr, 1);
	magiMsg->contentType = (uint8_t) contentType;

	msgBufPtr = msgBufPtr + 1;

	magiMsg->headers = NULL;
	while (len_optHeaders > 0) {
		headerOpt_t * opt_header = (headerOpt_t *) malloc(sizeof(headerOpt_t));

		char headerType;
		memcpy(&headerType, (char*) msgBufPtr, 1);
		opt_header->type = (uint8_t) headerType;

		msgBufPtr = msgBufPtr + 1;
		char headerLength;
		memcpy(&headerLength, (char*) msgBufPtr, 1);
		opt_header->len = (uint8_t) headerLength;

		msgBufPtr = msgBufPtr + 1;
		opt_header->value = malloc(opt_header->len);
		memcpy(opt_header->value, msgBufPtr, opt_header->len);
		opt_header->next = NULL;

		msgBufPtr = msgBufPtr + opt_header->len;

		if (magiMsg->headers == NULL) {
			magiMsg->headers = opt_header;
		} else {
			/*add at the end*/
			/*update list_t - groups/nodes/docks TODO*/
			headerOpt_t* tmp = magiMsg->headers;
			while (tmp->next != NULL)
				tmp = tmp->next;
			tmp->next = opt_header;
		}

		len_optHeaders = len_optHeaders - 2 - opt_header->len;
	}

	int dataLen = totalLength - headerLength - 2;
	log_debug(logger, "Magi Message Data Length: %d", dataLen);

	log_debug(logger, "Message content type: %d", magiMsg->contentType);

	switch (magiMsg->contentType) {
	case YAML:
		log_debug(logger, "Received YAML encoded message");
		magiMsg->data = (char*) malloc(dataLen + 1);
		strncpy(magiMsg->data, msgBufPtr, dataLen);
		magiMsg->data[dataLen] = '\0';
		log_debug(logger, "MAGI Message Data: %s", magiMsg->data);
		break;
	default:
		log_info(logger, "Content Type not supported\n");
		return NULL;
	}

	exitlog(logger, __func__, __FILE__, __LINE__);
	return magiMsg;
}

/**************************************************************************************
 * Function: encodeMagiMessage
 * This function returns a serialized buffer from MAGIMessage structure
 * This function allocates memory for the buffer. This should be freed by the caller.
 *
 * |TotalLength - 4B | HeaderLength - 2B | MsgID - 4B | Flags - 1B | ContentType - 1B|
 * -------------------------------------
 * Content         Size      Description
 * =============== ========  ===========
 * Length          4 bytes   header + data not including 4 length bytes
 * Header Length   2 bytes   length of just the header not including this length value
 * Identifier      4 bytes   this uniquely identifies any packet from the source
 * Flags           1 byte    Message Flags
 * ContentType     1 byte    Indicates generic format of content
 * Headers         variable  type-length-value options (byte, byte, variable)
 * Data            variable  the actual message data
 * =============== ========  ===========
 **************************************************************************************/
char* encodeMagiMessage(MAGIMessage_t* magiMsg, uint32_t* bufLen) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	uint16_t headerLength = 4 + 2 + calMagiMsgHdrLen(magiMsg->headers);
	uint32_t totalLength = 2 + headerLength + strlen(magiMsg->data);

	//Total Length does not include 4 bytes of message id
	char* msgBuf = (char*) malloc(totalLength + 4);
	msgBuf[0] = '\0';

	*bufLen = totalLength + 4;

	char* msgBufPtr = msgBuf;

	//TotalLength
	totalLength = htonl(totalLength);
	memcpy(msgBufPtr, &(totalLength), 4);
	msgBufPtr += 4;

	//HeaderLength
	if (headerLength == 0)
		memset(msgBufPtr, 0, 2);
	else {
		headerLength = htons(headerLength);
		memcpy(msgBufPtr, &headerLength, 2);
	}
	msgBufPtr += 2;

	//MsgID
	if (magiMsg->id == 0)
		memset(msgBufPtr, 0, 4);
	else {
		uint32_t msgId = htonl(magiMsg->id);
		memcpy(msgBufPtr, &msgId, 4);
	}
	msgBufPtr += 4;

	//Flags
	memcpy(msgBufPtr, &(magiMsg->flags), 1);
	msgBufPtr += 1;

	//ContentType
	memcpy(msgBufPtr, &(magiMsg->contentType), 1);
	msgBufPtr += 1;

	headerOpt_t * header = magiMsg->headers;
	while (header != NULL) {
		memcpy((void*) msgBufPtr, (void*) &header->type, 1);
		msgBufPtr += 1;
		memcpy((void*) msgBufPtr, (void*) &header->len, 1);
		msgBufPtr += 1;
		memcpy((void*) msgBufPtr, (void*) header->value, header->len);
		msgBufPtr += header->len;
		header = header->next;
	}

	memcpy((void*) msgBufPtr, (void*) magiMsg->data, strlen(magiMsg->data));

	exitlog(logger, __func__, __FILE__, __LINE__);
	return msgBuf;
}

/**
 * Create a partially filled MAGI message.
 *
 * @param srcdock if not null, the message srcdock is set to this
 * @param node if not null, node is added to the list of destination nodes
 * @param group if not null, group is added to the list of destination groups
 * @param dstdock if not null, dstdock is add to the list of destination docks
 * @param contenttype the contenttype for the data as specified in the Messenger interface
 * @param data encoded data for the message or null for none.
 */
MAGIMessage_t* createMagiMessage(char* srcdock, char* node, char* group,
		char* dstdock, contentType_t contenttype, char* data) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	MAGIMessage_t* magiMsg = allocateInitializedMagiMessage();

	if (srcdock)
		insert_header(magiMsg, SRCDOCK, strlen(srcdock) + 1, srcdock);
	if (node)
		insert_header(magiMsg, DSTNODES, strlen(node) + 1, node);
	if (group)
		insert_header(magiMsg, DSTGROUPS, strlen(group) + 1, group);
	if (dstdock)
		insert_header(magiMsg, DSTNODES, strlen(dstdock) + 1, dstdock);

	// MAGI message data.
	magiMsg->contentType = contenttype;
	magiMsg->data = (char*) malloc(strlen(data) + 1);
	strcpy(magiMsg->data, data);

	exitlog(logger, __func__, __FILE__, __LINE__);
	return magiMsg;
}

MAGIMessage_t* allocateInitializedMagiMessage() {
	MAGIMessage_t* magiMsg = (MAGIMessage_t*) malloc(sizeof(MAGIMessage_t));
	magiMsg->id = 0;
	magiMsg->flags = 0;
	magiMsg->contentType = 0;
	magiMsg->data = NULL;
	magiMsg->dstGroups = NULL;
	magiMsg->dstNodes = NULL;
	magiMsg->dstDocks = NULL;
	magiMsg->src = NULL;
	magiMsg->srcDock = NULL;
	magiMsg->headers = NULL;
	return magiMsg;
}

void freeMagiMessage(MAGIMessage_t* msg){
	entrylog(logger, __func__, __FILE__, __LINE__);
	if(msg != NULL){
		freeList(msg->dstGroups);
		freeList(msg->dstNodes);
		freeList(msg->dstDocks);

		headerOpt_t* header;
		while(msg->headers != NULL){
			header = msg->headers;
			msg->headers = header->next;
			free(header->value);
			free(header);
		}

		if(msg->data != NULL){
			free(msg->data);
		}
		free(msg);
	}
	exitlog(logger, __func__, __FILE__, __LINE__);
}
