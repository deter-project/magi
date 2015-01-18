#include "AgentRequest.h"
#include "MAGIMessage.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "logger.h"
extern Logger* logger;
/*TODO : #MACRO definitions for all length values + memory error checks*/

/****************************************************
 * Helper function to calculate options total length
 * ***************************************************/
uint32_t calculate_hlen(AgentRequest_t* msg)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	uint32_t len =0;
	AgentRequestOptions_t* tmp = msg->options;
	while(tmp)
	{
		if(tmp->options == TIME_STAMP)	
			len+=4;
		len+=2; /*key and length fields*/		
		tmp = tmp->next;
	}
	log_debug(logger,"Exiting function: %s\n",__func__);
	return len;

}
/****************************************************
 * Helper function to add options header
 * ***************************************************/
void add_options(AgentRequestOptions_t** op, char* key, uint32_t len, char* value)
{
	AgentOptions_t type;
	if(!strcmp(key,"ACK"))
	{
		type = ACK;
	}
	else if(!strcmp(key,"SOURCE_ORDERING"))
	{
		type = SOURCE_ORDERING; 
	}	
	else if(!strcmp(key,"TIME_STAMP"))
	{
		type = 	TIME_STAMP;
	}
	else
	{
		log_info(logger,"Invalid AgentRequest type\n");
		return;
	}	
	if(strlen(value) > 4)
		log_info(logger,"Option value cannot be greater than 4 Bytes..Truncated\n");
	AgentRequestOptions_t* tmp = (AgentRequestOptions_t*)malloc(sizeof(AgentRequestOptions_t));
	tmp->options = type;
	tmp->len = 0;
	if(tmp->options == TIME_STAMP)
		tmp->len = 4;
	tmp->value = value; 

	tmp->next = *op;
	*op = tmp;
	
}
/**************************************************************************************
 * Function: AgentEncode
 * This function returns a buffer with serialized AgentRequest structure contents and 
 * updates size of the buffer returned
 * This function allocates memory for the buffer. This should be freed by the caller.
 * *************************************************************************************/

char * AgentEncode(AgentRequest_t* msg,uint32_t* bufLen)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	/*Encoding the AgentRequest Header*/
	/*First find the length of all options and pack options*/
	uint16_t headerlen =0;
	uint32_t totalLen =0;
	char * data = NULL;
	uint32_t hlen = calculate_hlen(msg);
	/*TotalLength-4, HeaderLen-2B, RequestType -1B,options*/
	char * hbuf = (char*)malloc(8+hlen+4+2+1);/*total size of the request message header*/

	/*Copy the preamble*/ /*Anything which goes out on the wire has to have this*/
	memcpy(hbuf,"MAGI\x88MSG",8);

	/*Copy request type*/
	memcpy(hbuf+8+4+2,&msg->reqType,1);

	/*Copy all the options into the buffer */
	AgentRequestOptions_t* tmp = msg->options;
	while(tmp)
	{
		/*Special case for timestamp*/
		if(tmp->options == TIME_STAMP)
		{
			/*key,4Bytes,value*/
			/*Copy the key*/
			uint8_t option = tmp->options;
			memcpy(hbuf+8+6+1+headerlen,&option,1);
			headerlen += 1;
			/*Copy the length*/
			uint8_t zlen = tmp->len;
			memcpy(hbuf+8+6+1+headerlen,&zlen,1);
			headerlen +=1;
			/*TODO : Value int?*/			
			memcpy(hbuf+8+6+1+headerlen,tmp->value,4);
			headerlen +=4;
							
		}
		else
		{
			uint8_t option =tmp->options;
			memcpy(hbuf+8+6+1+headerlen,&option,1);
			headerlen += 1;
			uint8_t zlen = htonl(0);

			memcpy(hbuf+8+6+1+headerlen,&zlen,1);
			headerlen+=1;

		}
		tmp = tmp->next;
	}
	char* temp;
	/*Copy the data*/
	if(msg->reqType == 5)
	{
		/*Get the length of the Magi message structure*/
		MAGIMessage_t* magiMsg = (MAGIMessage_t*)msg->data;
		/*MAGI header and data is encoded*/
		uint32_t bufLen;	
		MAGIMessageEncode(&data,magiMsg,&bufLen);
		totalLen+=bufLen;

	}
	else
	{
		/*Data is a string*/
		totalLen = strlen(msg->data); /*Should '\0' be included?*/
		//strcpy(temp,msg->data); /*This can be on the heap or stack*/
		data = msg->data;
	}

	/*Data and options have been copied to the buffer*/
	headerlen += 1;
	totalLen = totalLen+headerlen+2;
	totalLen = htonl(totalLen);
	headerlen = htons(headerlen);
	/*Copy headerLen and totalLen into the buffer*/
	memcpy(hbuf+8,&totalLen,4);
	memcpy(hbuf+8+4,&headerlen,2);
	totalLen = ntohl(totalLen);
        headerlen = ntohs(headerlen);
	/*Header Encode complete. Add (Magi header+Magi data encoded buffer)*/
	char* buf = (char*)malloc(totalLen+4+8); /*Final buffer*/
	memcpy(buf,hbuf,headerlen+6+8); /*Only the header+options part */
	memcpy(buf+headerlen+6+8,data,totalLen - headerlen - 2); /*Just the data*/
	*bufLen = totalLen+4+8;
	log_debug(logger,"Exiting function: %s\n",__func__);
	return buf;	
}



/**************************************************************************************
 * Function: AgentDecode
 * This function returns a AgentRequest structure by deserializing the incoming stream
 * This function allocates memory for the structure. This should be freed by the caller.
 * *************************************************************************************/

AgentRequest_t*  AgentDecode(char* buf)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	char magi[8]; 
	memcpy(magi,buf,8);
	if(strncmp(magi,"MAGI\x88MSG",8))
	{
		log_info(logger,"Invalid Agent msg\n");
		return NULL;
	} //taken care in the transport module
	
	AgentRequest_t *msg = (AgentRequest_t*) malloc(sizeof(AgentRequest_t));
	uint32_t totalLen;
	uint16_t headerLen;
	
	buf = buf+8;
	
	/*Extract lengths*/
	memcpy(&totalLen,buf,4);
	memcpy(&headerLen,buf+4,2);
	totalLen = ntohl(totalLen);
	headerLen = ntohs(headerLen);
	char typ;
	memcpy(&typ,(char*)buf+6,1);
	msg->reqType = (uint8_t)typ;
	char* data = buf+2+4+headerLen;
	/*Extract data*/
	switch(msg->reqType)
	{
		case MESSAGE: {
				log_info(logger,"Agent Decode: Calling MAGI Decode...\n");
				MAGIMessage_t* Magimsg = MAGIMessageDecode(data);
				msg->data = (char*)Magimsg;
				break;		
			      }

		case JOIN_GROUP:
		case LEAVE_GROUP:
		case LISTEN_DOCK:
		case UNLISTEN_DOCK:
			      	msg->data = (char*)malloc(strlen(data));
				strcpy(msg->data,data);
				break;
		default: log_info(logger,"Invalid reqType\n");
			 return NULL;

	}

	/*Get the options headers*/
	if(headerLen < 1){
		/*No Options field.*/
		/*ToDo*/

	}
	else
	{
	int optionsLen = headerLen -1; 
	char* tmp = buf+4+2+1;
	msg->options = NULL;
	while(optionsLen)
	{
		AgentRequestOptions_t* header = (AgentRequestOptions_t*) malloc(sizeof(AgentRequestOptions_t));
		memcpy(&header->options,tmp,1);
		//header->options = ntohl(header->options);
		tmp = tmp+1;
		memcpy(&header->len,tmp,1);
		//header->len = ntohl(header->len);
		tmp+=1;
		if(header->len && header->options == TIME_STAMP)
		{
			if(header->len != 4)
				log_info(logger,"Invalid header length for TIME_STAMP\n");
			optionsLen -=header->len;
			header->value = (char*)malloc(4);
			memcpy(header->value,tmp,4);
			tmp+=4;
		}
		else
		{
			header->len =0;
			header->value = NULL;
		}
		//tmp+=header->len;
		optionsLen -= 2;
		/*adding at the start*/
		header->next = msg->options;
		msg->options = header;
	}
	}
	log_debug(logger,"Exiting function: %s\n",__func__);
	return msg;

}
