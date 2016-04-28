#include "AgentRequest.h"
#include "MAGIMessage.h"
#include "Logger.h"

#include <string.h>

extern Logger* logger;

const char* preamble = "MAGI\x88MSG";

/*TODO : #define for all length values + memory error checks*/

/****************************************************
 * Helper function to calculate options total length
 * ***************************************************/
uint16_t calAgentReqHdrLen(AgentRequest_t* req) {
	//entrylog(logger, __func__, __FILE__, __LINE__);
	uint16_t len = 0;
	AgentRequestOptions_t* tmp = req->options;
	while (tmp != NULL) {
		if (tmp->options == TIME_STAMP)
			len += 4;
		len += 2; /*key and length fields*/
		tmp = tmp->next;
	}
	//exitlog(logger, __func__, __FILE__, __LINE__);
	return len;
}

/****************************************************
 * Helper function to add options header
 * ***************************************************/
void add_options(AgentRequestOptions_t** op, char* key, uint32_t len,
		char* value) {
	AgentOptions_t type;
	if (!strcmp(key, "ACK")) {
		type = ACK;
	} else if (!strcmp(key, "SOURCE_ORDERING")) {
		type = SOURCE_ORDERING;
	} else if (!strcmp(key, "TIME_STAMP")) {
		type = TIME_STAMP;
	} else {
		log_error(logger, "Invalid AgentRequest type");
		return;
	}

	if (strlen(value) > 4)
		log_info(logger,
				"Option value cannot be greater than 4 Bytes..Truncated\n");

	AgentRequestOptions_t* tmp = (AgentRequestOptions_t*) malloc(
			sizeof(AgentRequestOptions_t));
	tmp->options = type;
	tmp->len = 0;
	if (tmp->options == TIME_STAMP)
		tmp->len = 4;
	tmp->value = value;

	tmp->next = *op;
	*op = tmp;
}

/**************************************************************************************
 * Function: encodeAgentRequest
 * This function returns a buffer with serialized AgentRequest structure contents and 
 * updates size of the buffer returned
 * This function allocates memory for the buffer. This should be freed by the caller.
 *
 * |TotalLength - 4B | HeaderLength - 2B | RequestType - 1B|
 * -------------------------------------
 * Content         Size      Description
 * =============== ========  ===========
 * Length          4 bytes   header + data
 * Header Length   2 bytes   length of just the header not including this length value
 * RequestType     1 byte    Indicates generic format of content
 * Headers         variable  type-length-value options (byte, byte, variable)
 * Data            variable  the actual message data
 * =============== ========  ===========
 * *************************************************************************************/

char* encodeAgentRequest(AgentRequest_t* req, uint32_t* bufLen) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	/*Encoding the AgentRequest Header*/
	/*First find the length of all options and pack options*/
	uint16_t headerlen = calAgentReqHdrLen(req);
	uint32_t totalLen = 0;

	/*total size of the request message header*/
	int headerBufSize = 4 + 2 + 1 + headerlen;
	char* hbuf = (char*) malloc(headerBufSize);

	headerlen += 1;

	totalLen = req->dataLength + headerlen + 2;

	char* hdrBufPtr = hbuf;

	//TotalLength
	totalLen = htonl(totalLen);
	memcpy(hdrBufPtr, &totalLen, 4);
	hdrBufPtr += 4;

	//HeaderLength
	headerlen = htons(headerlen);
	memcpy(hdrBufPtr, &headerlen, 2);
	hdrBufPtr += 2;

	//RequestType
	memcpy(hdrBufPtr, &req->reqType, 1);
	hdrBufPtr += 1;

	/*Copy all the options into the buffer */
	AgentRequestOptions_t* tmp = req->options;
	while (tmp) {
		/*Special case for timestamp*/
		if (tmp->options == TIME_STAMP) {
			/*key,length,value*/
			/*Copy the key*/
			uint8_t option = tmp->options;
			memcpy(hdrBufPtr, &option, 1);
			hdrBufPtr += 1;

			/*Copy the length*/
			uint8_t zlen = tmp->len;
			memcpy(hdrBufPtr, &zlen, 1);
			hdrBufPtr += 1;

			/*TODO : Value int?*/
			memcpy(hdrBufPtr, tmp->value, 4);
			hdrBufPtr += 4;
		} else {
			uint8_t option = tmp->options;
			memcpy(hdrBufPtr, &option, 1);
			hdrBufPtr += 1;

			uint8_t zlen = htonl(0);
			memcpy(hdrBufPtr, &zlen, 1);
			hdrBufPtr += 1;
		}
		tmp = tmp->next;
	}

	totalLen = ntohl(totalLen);
	headerlen = ntohs(headerlen);

	//Agent Request Buffer
	//8 bytes of preamble + 4 bytes of total length value + Actual total length
	char* encodedAgentReq = (char*) malloc(8 + 4 + totalLen);

	char* reqBufPtr = encodedAgentReq;

	//Preamble - Anything which goes out on the wire has to have this
	memcpy(reqBufPtr, preamble, 8);
	reqBufPtr += 8;

	//Header + Options
	memcpy(reqBufPtr, hbuf, 4 + 2 + headerlen);
	free(hbuf);
	reqBufPtr = reqBufPtr + 4 + 2 + headerlen;

	//Data
	memcpy(reqBufPtr, req->data, req->dataLength);

	*bufLen = totalLen + 4 + 8;
	exitlog(logger, __func__, __FILE__, __LINE__);
	return encodedAgentReq;
}

/**************************************************************************************
 * Function: decodeAgentRequest
 * This function returns a AgentRequest structure by deserializing the incoming stream
 * This function allocates memory for the structure. This should be freed by the caller.
 * *************************************************************************************/
AgentRequest_t* decodeAgentRequest(char* encodedAgentReq) {
	entrylog(logger, __func__, __FILE__, __LINE__);

	char* reqBufPtr = encodedAgentReq;

	char firstEight[8];
	memcpy(firstEight, reqBufPtr, 8);
	if (strncmp(firstEight, preamble, 8)) {
		log_error(logger,
				"Invalid Agent Request. Does not begin with preamble");
		return NULL;
	} //taken care in the transport module
	reqBufPtr = reqBufPtr + 8;

	/*Extract lengths*/
	uint32_t totalLen;
	memcpy(&totalLen, reqBufPtr, 4);
	totalLen = ntohl(totalLen);
	reqBufPtr = reqBufPtr + 4;

	uint16_t headerLen;
	memcpy(&headerLen, reqBufPtr, 2);
	headerLen = ntohs(headerLen);
	reqBufPtr = reqBufPtr + 2;

	AgentRequest_t* req = (AgentRequest_t*) malloc(sizeof(AgentRequest_t));
	char reqType;
	memcpy(&reqType, (char*) reqBufPtr, 1);
	req->reqType = (uint8_t) reqType;

	log_debug(logger, "Agent Request Total Length: %d", totalLen);
	log_debug(logger, "Agent Request Header Length: %d", headerLen);

	int dataLen = totalLen - headerLen;
	log_debug(logger, "Agent Request Data Length: %d", dataLen);

	char* data = reqBufPtr + headerLen;
	req->data = (char*) malloc(dataLen);
	memcpy(req->data, data, dataLen);

	req->options = NULL;

	/*Get the options headers*/
	if (headerLen == 1) {
		/*No Options field.*/
		log_debug(logger, "No option fields.");
	} else {

		reqBufPtr = reqBufPtr + 1;
		int optionsLen = headerLen - 1;

		while (optionsLen) {
			AgentRequestOptions_t* header = (AgentRequestOptions_t*) malloc(
					sizeof(AgentRequestOptions_t));

			memcpy(&header->options, reqBufPtr, 1);
			header->options = ntohl(header->options);
			reqBufPtr += 1;

			memcpy(&header->len, reqBufPtr, 1);
			header->len = ntohl(header->len);
			reqBufPtr += 1;

			optionsLen -= 2;

			if (header->len && header->options == TIME_STAMP) {
				if (header->len != 4)
					log_error(logger, "Invalid header length for TIME_STAMP\n");

				header->value = (char*) malloc(header->len);
				memcpy(header->value, reqBufPtr, header->len);
				reqBufPtr += header->len;
				optionsLen -= header->len;

			} else {
				header->len = 0;
				header->value = NULL;
			}

			/*adding at the start*/
			header->next = req->options;
			req->options = header;
		}
	}

	exitlog(logger, __func__, __FILE__, __LINE__);
	return req;
}

/*************************************
 *
 **************************************/
AgentRequest_t* createAgentRequest(AgentRequestType_t reqType, char* data,
		uint32_t dataLength) {
	entrylog(logger, __func__, __FILE__, __LINE__);
	AgentRequest_t* req = (AgentRequest_t*) malloc(sizeof(AgentRequest_t));
	req->reqType = reqType;
	req->options = NULL;
	req->data = (char*) malloc(dataLength);
	memcpy(req->data, data, dataLength);
	req->dataLength = dataLength;
	exitlog(logger, __func__, __FILE__, __LINE__);
	return req;
}

void freeAgentRequest(AgentRequest_t* req) {
	if (req != NULL) {
		AgentRequestOptions_t* options;
		while (req->options != NULL) {
			options = req->options;
			req->options = options->next;
			free(options);
		}
		if (req->data != NULL) {
			free(req->data);
		}
		free(req);
	}
}
