#include "MAGIMessage.h"
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include "AgentMessenger.h"
extern char* hostName;

void addList(list_t** list, char* name)
{
	list_t* temp = (list_t*)malloc(sizeof(list_t));
	temp->name = malloc(strlen(name)+1);
	strcpy(temp->name,name);
	temp->next = *list;
	*list = temp;
 //	return list; 

}


int calHlen(headerOpt_t* headers)
{
	int len =0;
	while(headers)
	{
		len+=headers->len;
		len+=2;
		headers= headers->next;
	}
	return len;

}

/****************************************************
 * Helper function to insert header into a list
 * ***************************************************/
void insert_header( MAGIMessage_t* msg, uint8_t type, uint8_t len, char * value)
{
	
	headerOpt_t * list = msg->headers;
	headerOpt_t * opt_header = (headerOpt_t *) malloc(sizeof(headerOpt_t));
	opt_header->type = type;
	opt_header->len = strlen(value);
	if(len == 0 || value == NULL)
		{
			printf("illegal header\n");
		}
	opt_header->value = malloc(opt_header->len+1);
	memcpy(opt_header->value,value,strlen(value));
	opt_header->next = NULL;

	if(list == NULL)
	{
		list = opt_header;
	}
	else
	{
		headerOpt_t * tmp = list;
		while(tmp->next)
			tmp = tmp->next;			
		tmp->next = opt_header;
	}
	if(type == DSTDOCKS)
		addList(&msg->dstDocks,value);
	else if(type == DSTGROUPS)
		addList(&msg->dstGroups,value);
	else if(type == DSTNODES)
		addList(&msg->dstNodes,value);
	msg->headers = list;
}

char *trimwhitespace(char *str)
{
  char *end;

  // Trim leading space
  while(isspace(*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim traili space
  end = str + strlen(str) - 1;
  while(end > str && isspace(*end)) end--;

  // Write new null terminator
  *(end+1) = 0;

  return str;
}



void decodeYAML(char* msg, char** func, char**args,char** event)
{
	char* buf = (char*)malloc(strlen(msg));
        strncpy(buf,msg,strlen(msg));

        char*tkn,*temp;
        char* bufp =buf;
        int j =0,cnt =0,i=0;
	int max = strlen(buf);
        for(j =0;j<strlen(buf);j++)
        {
                if(buf[j] == '\'' || buf[j] == '\"')
                        buf[j] = ' ';
        }

nxt:    while(buf)
     	{
		printf("parsing line\n");
		cnt =0;
        	j=0;
        	while(buf[j] != '\n')
        	{        
			cnt++; j++;
			if(j == max){
				printf("something wrong in YAML parsing of string:\n %s\n",msg);
				return;	}
		}
	 	//read 1 line
        	char* line ;
		line =NULL;
        	line = malloc(cnt);
        	strncpy(line,buf,cnt);
		printf("line parsed: %s\n",line);
       	 	line[cnt]='\0';
        	char * ck;
        	if((ck = strstr(line, "method"))!=NULL)
        	{
			printf("YAML: Method\n");
                	//This line has method
                	tkn = strtok(line,":");
                	tkn = strtok(NULL,":,");
                	//func name
                	printf("Function name : %s\n",tkn);
                	tkn = trimwhitespace(tkn);
			printf("Copying data\n");
                	*func = (char*)malloc(strlen(tkn)+1);
                	strncpy(*func,tkn,strlen(tkn));
			
                //	*func[strlen(tkn)] = '\0';
	     //		printf("Finished parsing method\n");
        	}
        	else if((ck = strstr(line, "args"))!=NULL)
        	{
			printf("YAML: Args\n");
			//args: {key1: '1', key2: '2'} 
                	tkn = strtok(line,":");//key - args     
                	if(tkn ==NULL)
			{
				buf = buf+cnt+1;
                        	goto nxt;
                	}
			tkn = strtok(NULL,"}");//val -> args list
                	char* tstr =(char*)malloc(strlen(tkn));
                	strcpy(tstr,tkn);
                	char *t1 = strchr(tstr,'{');
                	if (t1)
                	{
                        	t1++;
                       	 	tstr = t1;
                	}

                // tstr = " key1: 1,key2:2 " or " key1:1 "
                	char * t;
                	t= strtok(tstr,",:");//key
                	do
                	{
                                //1 key value pair
                        	t = strtok(NULL,":,%"); //value
                        	if(t ==NULL)
                                	return;
				t = trimwhitespace(t);
                        	printf("arg value[%d]: %d\n",i, atoi(t));
                       		t = trimwhitespace(t);
                        	temp = (char*)malloc(strlen(t));
                        	strncpy(temp,t,strlen(t)+1);
                        	args[i] = temp;
                        	i++;
                	}while((t= strtok(NULL,",:%"))!=NULL); //key
    			args[i] =NULL;
			printf("Parsed all args\n");
                }

	//
	else if((ck = strstr(line, "trigger"))!=NULL)
	{
		printf("YAML: trigger\n");
		tkn = strtok(line,":");
                tkn = strtok(NULL,":,");
                //event name
                printf("Event name : %s\n",tkn);
                tkn = trimwhitespace(tkn);
                *event = (char*)malloc(strlen(tkn)+1);
                strncpy(*event,tkn,strlen(tkn));
     //           *event[strlen(tkn)] = '\0';

	}
        buf = buf+cnt+1;

    }
	
        printf("Finished YAML parse\n");

}




/**************************************************************************************
 * Function: MAGIMessageDecode
 * This function returns MAGIMessage structure from buffer(deserialize)
 * This function allocates memory for the structure. This should be freed by the caller.
 **************************************************************************************/
MAGIMessage_t *MAGIMessageDecode(char* msgBuf)
{
	printf("Entering MAGIMessageDecode\n");

	/*Caller has realized it is a MAGI message*/
	MAGIMessage_t* MAGImsg = (MAGIMessage_t*) malloc(sizeof(MAGIMessage_t));
	if(MAGImsg == NULL)
	{
		printf("malloc failed\n");
		exit(0);
	}
//printf("Inside MAGI Decode\n");
	MAGImsg->headers =NULL; 
	char* parser = msgBuf;
//printf("parser points to : %x\n",parser);
	int len_optHeaders;

	//parser = parser+8;
	/*Now parser points to length*/
	memcpy(&MAGImsg->length,parser,4);
printf("Before ntohl, len :%d\n",MAGImsg->length);
	MAGImsg->length = ntohl(MAGImsg->length);

	parser = parser+ 4; /*HeaderLen*/
//printf("parser points to : %x\n",parser);
	memcpy(&MAGImsg->headerLength,parser,2);
	MAGImsg->headerLength = ntohs(MAGImsg->headerLength);
printf("Got length: %d\n headerLength: %d \n",MAGImsg->length,MAGImsg->headerLength);
	len_optHeaders = MAGImsg->headerLength -6; 

	parser = parser+2;/*points to Identifier*/
	memcpy(&MAGImsg->id,(void*)parser,4);
	MAGImsg->id = ntohl(MAGImsg->id);

	parser = parser + 4; /*Flags*/
	memcpy(&MAGImsg->flags,(void*)parser,1);

	parser = parser + 1; /*content type*/
	memcpy(&MAGImsg->contentType,(void*)parser,1);
	parser = parser +1;	
printf("\nAgent Magi decode:  optHeaderLen: %d\n id = %d\n contentType = %d\n", len_optHeaders,MAGImsg->id,MAGImsg->contentType);
	while(len_optHeaders > 0){
		headerOpt_t * opt_header = (headerOpt_t *) malloc(sizeof(headerOpt_t)); 

		
		memcpy((void*)&opt_header->type,(void*)parser,1);

		parser = parser +1; 
		memcpy((void*)&opt_header->len,(void*)parser,1);

		parser = parser+1;
		opt_header->value = malloc(opt_header->len);
		memcpy(opt_header->value,(void*)parser,opt_header->len);
		opt_header->next = NULL;
		
		parser = parser + opt_header->len;

		if(MAGImsg->headers == NULL)
		{			
			MAGImsg->headers = opt_header;
		}
		else
		{
			/*add at the end*/
			/*update list_t - groups/nodes/docks TODO*/
			headerOpt_t* tmp =  MAGImsg->headers;
			while(tmp->next != NULL)
				tmp = tmp->next;
			tmp->next = opt_header;
		}

		len_optHeaders = len_optHeaders - 2 - opt_header->len;
		
	}
//printf("\nAgent:Checking data part of MAGI msg received: %d\n", MAGImsg->contentType);	
	MAGImsg->data = (char*)malloc(MAGImsg->length - MAGImsg->headerLength - 2);

	switch(MAGImsg->contentType)
	{
		case YAML:
		       		memcpy(MAGImsg->data,parser,MAGImsg->length - MAGImsg->headerLength - 2 );
				printf("Received YAML message...\n");	
				char* event =NULL;			
				decodeYAML(MAGImsg->data,&(MAGImsg->funcArgs.func),&(MAGImsg->funcArgs.args),&event);

				printf("Calling function\n");
				int retVal = call_function(MAGImsg->funcArgs.func,&(MAGImsg->funcArgs.args));
				if(event)
				{
					printf("Sending out Trigger message\n");
					char ndata[250];
					sprintf(ndata,"{event: %s, result: %d, nodes: %s}",event,retVal,hostName);
					char* td = (char*)malloc(strlen(ndata)+1);
					strcpy(td,ndata);
					trigger("control", "control", YAML, td);
					printf("Sent a trigger message\n");
				}
				printf("exiting decode\n");
				break;

		default:
				printf("Content Type not supported\n");
				break;

	}
	printf("Exiting MAGIMessageDecode\n");
	return (MAGImsg);

}


/**************************************************************************************
 * Function: MAGIMessageEncode
 * This function returns a serialized buffer from MAGIMessage structure
 * This function allocates memory for the buffer. This should be freed by the caller.
 **************************************************************************************/

char * MAGIMessageEncode1(MAGIMessage_t* MAGImsg, uint32_t* bufLen)
{
	char * msgBuf=NULL;
	printf("Entering MAGIMessageEncode\n");
	//printf("MAGI len:%d\n",MAGImsg->length);
	//printf("MAGI data:%s\n",MAGImsg->data);
	msgBuf = (char*)malloc(MAGImsg->length+4);
	/*err check*/
	char* tmp = msgBuf;

	/*Copy the preamble*/
/*	memcpy(msgBuf,"MAGI\x88MSG",8);
	tmp += 8;*/

	uint32_t conv = htonl(MAGImsg->length);
	memcpy(tmp, &(conv), 4);
	tmp+=4;
	
	if(MAGImsg->headerLength == 0)
		memset(tmp,0,2);
	else{
		uint16_t conv1 = htons(MAGImsg->headerLength);
		memcpy(tmp, &conv1, 2);
	}
	tmp+=2;
	
	if(MAGImsg->id == 0)
		memset(tmp,0,4);
	else
	{
		conv = htonl(MAGImsg->id);
		memcpy(tmp, &conv, 4);
	}
	tmp+=4;

	memcpy(tmp, &(MAGImsg->flags), 1);
	tmp+=1;

	memcpy(tmp,&(MAGImsg->contentType), 1);
	tmp+=1;


	/*tmp pointing to start of opt_headers*/
	headerOpt_t * header = MAGImsg->headers;
	/*No error checking of the total size - TODO*/
	while(header != NULL)
	{
	memcpy((void*)tmp,(void*)&header->type,1);
	tmp+=1;
	memcpy((void*)tmp,(void*)&header->len,1);
	tmp+=1;
	memcpy((void*)tmp,(void*)header->value,header->len);
	tmp+=header->len;
	header = header->next;
	}
//printf("Copying memory in MAGIEnc\n");
	memcpy((void*)tmp,(void*)MAGImsg->data, MAGImsg->length - MAGImsg->headerLength - 2 );
	*bufLen = MAGImsg->length+4;
	//printf("msgBuf: %s\n",msgBuf);
//printf("Done MAGI  ENC\n");


	printf("Exiting MAGIMessageEncode\n");

	return msgBuf;

}

char * MAGIMessageEncode(char** b,MAGIMessage_t* MAGImsg, uint32_t* bufLen)
{
	char * msgBuf;
	printf("Entering MAGIMessageEncode\n");
	//printf("MAGI len:%d\n",MAGImsg->length);
	//printf("MAGI data:%s\n",MAGImsg->data);
	msgBuf = (char*)malloc(MAGImsg->length+4);
	/*err check*/
	char* tmp = msgBuf;

	/*Copy the preamble*/
/*	memcpy(msgBuf,"MAGI\x88MSG",8);
	tmp += 8;*/

	uint32_t conv = htonl(MAGImsg->length);
	memcpy(tmp, &(conv), 4);
	tmp+=4;
	
	if(MAGImsg->headerLength == 0)
		memset(tmp,0,2);
	else{
		uint16_t conv1 = htons(MAGImsg->headerLength);
		memcpy(tmp, &conv1, 2);
	}
	tmp+=2;
	
	if(MAGImsg->id == 0)
		memset(tmp,0,4);
	else
	{
		conv = htonl(MAGImsg->id);
		memcpy(tmp, &conv, 4);
	}
	tmp+=4;

	memcpy(tmp, &(MAGImsg->flags), 1);
	tmp+=1;

	memcpy(tmp,&(MAGImsg->contentType), 1);
	tmp+=1;


	/*tmp pointing to start of opt_headers*/
	headerOpt_t * header = MAGImsg->headers;
	/*No error checking of the total size - TODO*/
	while(header != NULL)
	{
	memcpy((void*)tmp,(void*)&header->type,1);
	tmp+=1;
	memcpy((void*)tmp,(void*)&header->len,1);
	tmp+=1;
	printf("header->len : %d\nvalue:%s\n",header->len,header->value);
	memcpy((void*)tmp,(void*)header->value,header->len);
	tmp+=header->len;
	header = header->next;
	}
//printf("Copying memory in MAGIEnc\n");
	memcpy((void*)tmp,(void*)MAGImsg->data, MAGImsg->length - MAGImsg->headerLength - 2 );
	*bufLen = MAGImsg->length+4;
	//printf("msgBuf: %s\n",msgBuf);
//printf("Done MAGI  ENC\n");


	printf("Exiting MAGIMessageEncode\n");
	*b = msgBuf;
	return msgBuf;

}

