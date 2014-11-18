#include "AgentRequest.h"
#include "MAGIMessage.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/*TODO : #define for all length values + memory error checks*/

/****************************************************
 * Helper function to calculate options total length
 * ***************************************************/
uint32_t calculate_hlen(AgentRequest_t* msg)
{
	uint32_t len =0;
	AgentRequestOptions_t* tmp = msg->options;
	while(tmp)
	{
		if(tmp->options == TIME_STAMP)	
			len+=4;
		len+=2; /*key and length fields*/		
		tmp = tmp->next;
	}
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
		printf("Invalid AgentRequest type\n");
		return;
	}	
	if(strlen(value) > 4)
		printf("Option value cannot be greater than 4 Bytes..Truncated\n");
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
	/*Encoding the AgentRequest Header*/
	/*First find the length of all options and pack options*/
	uint16_t headerlen =0;
	uint32_t totalLen =0;
	char * data = NULL;
	uint32_t hlen = calculate_hlen(msg);
	printf("hlen:%d\n",hlen);
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

	/*Copy the data*/
	if(msg->reqType == 5)
	{
		/*Get the length of the Magi message structure*/
		MAGIMessage_t* magiMsg = (MAGIMessage_t*)msg->data;
	//	totalLen = magiMsg->length+4+8; //total magi msg size
		/*MAGI header and data is encoded*/
		uint32_t bufLen;
		printf("Calling MAGIEncode\n");
		data = MAGIMessageEncode(magiMsg,&bufLen);
		printf("MAGIEncode Complete:%d\n",bufLen);
		totalLen+=bufLen;

	}
	else
	{
		/*Data is a string*/
		totalLen = strlen(msg->data); /*Should '\0' be included?*/
		data = msg->data; /*This can be on the heap or stack*/

	}

	/*Data and options have been copied to the buffer*/
printf("headerlen:%d\n",headerlen);
	headerlen += 1;
	totalLen = totalLen+headerlen+2;
	totalLen = htonl(totalLen);
	headerlen = htons(headerlen);
	/*Copy headerLen and totalLen into the buffer*/
	memcpy(hbuf+8,&totalLen,4);
	memcpy(hbuf+8+4,&headerlen,2);
	totalLen = ntohl(totalLen);
        headerlen = ntohs(headerlen);
printf("lengths calculated %d \n", totalLen);	
	/*Header Encode complete. Add (Magi header+Magi data encoded buffer)*/
	char* buf = (char*)malloc(totalLen+4+8); /*Final buffer*/
printf("headerlen: %d\n",headerlen);
//printf("strlen of data : %d\n",strlen(data));
	memcpy(buf,hbuf,headerlen+6+8); /*Only the header+options part */
//printf("buf: %s\nhbuf:%s\n",buf,hbuf);	
	memcpy(buf+headerlen+6+8,data,totalLen - headerlen - 2); /*Just the data*/
	*bufLen = totalLen+4+8;
	printf("returning enc msg\n");
	return buf;	
}



/**************************************************************************************
 * Function: AgentDecode
 * This function returns a AgentRequest structure by deserializing the incoming stream
 * This function allocates memory for the structure. This should be freed by the caller.
 * *************************************************************************************/

AgentRequest_t*  AgentDecode(char* buf)
{
	char magi[8]; 
	memcpy(magi,buf,8);
	if(strncmp(magi,"MAGI\x88MSG",8))
	{
		printf("Invalid Agent msg\n");
		return NULL;
	} //taken care in the transport module
	printf("In Agent Message decode\n");
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
printf("totalLen : %d\n headerLen: %d\n reqType: %d\n",totalLen,headerLen,msg->reqType);
	/*Extract data*/
//printf("data %s\n",data);
	switch(msg->reqType)
	{
		case MESSAGE: {
				printf("Agent Decode: Calling MAGI Decode...\n");
				//char* d = (char*)malloc(totalLen - headerLen - 1);
				//memcpy(d,data,totalLen - headerLen-1);
				//int test =0;
				//memcpy(&test,d,4); 
				//printf("MAGI message sending to MAGIDecode %d\n ",test);
				MAGIMessage_t* Magimsg = MAGIMessageDecode(data);
				msg->data = (char*)Magimsg;
				break;		
			      }

		case JOIN_GROUP:
		case LEAVE_GROUP:
		case LISTEN_DOCK:
		case UNLISTEN_DOCK:
				printf("data length %d\n",strlen(data));
			      	msg->data = (char*)malloc(strlen(data));
				strcpy(msg->data,data);
				printf("DATA in the msg %s",(char*)(msg->data));	
				break;
		default: printf("Invalid reqType\n");
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
				printf("Invalid header length for TIME_STAMP\n");
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
	printf("Agent Decode success\n");
	return msg;

}

/*int main()
{

MAGIMessage_t Mmsg;
Mmsg.headers = NULL;
Mmsg.length = 1234;
Mmsg.headerLength = 29;
Mmsg.id = 678;
Mmsg.flags = 4;
Mmsg.contentType = 5;

insert_header(&Mmsg.headers, 52,8,"Counters");
insert_header(&Mmsg.headers, 20,4,"GUIX");
insert_header(&Mmsg.headers,51,5,"nodes");

Mmsg.data = "some random YAML text";

/ *Agent mesg* /
AgentRequest_t Amsg;
memset(&Amsg,0,sizeof(Amsg));
Amsg.reqType = MESSAGE;
add_options(&Amsg.options,TIME_STAMP,strlen("timestamp"),"timestamp");
add_options(&Amsg.options,ACK,strlen("ackd"),"ackd");
Amsg.data = (char*)&Mmsg;

char* Abuf = AgentEncode(Amsg);
AgentRequest_t* Fmsg = AgentDecode(Abuf);

return 0;
}
*/
