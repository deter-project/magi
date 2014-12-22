#include "MAGIMessage.h"
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include "AgentMessenger.h"
#include "logger.h"
extern char* hostName;
extern Logger* logger;

void addList(list_t** list, char* name)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	list_t* temp = (list_t*)malloc(sizeof(list_t));
	temp->name = malloc(strlen(name)+1);
	strcpy(temp->name,name);
	temp->next = *list;
	*list = temp;
	log_debug(logger,"Exiting function: %s\n",__func__);

}


int calHlen(headerOpt_t* headers)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	int len =0;
	while(headers)
	{
		len+=headers->len;
		len+=2;
		headers= headers->next;
	}
	log_debug(logger,"Exiting function: %s\n",__func__);
	return len;
}

/****************************************************
 * Helper function to insert header into a list
 * ***************************************************/
void insert_header( MAGIMessage_t* msg, uint8_t type, uint8_t len, char * value)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);	
	headerOpt_t * list = msg->headers;
	headerOpt_t * opt_header = (headerOpt_t *) malloc(sizeof(headerOpt_t));
	opt_header->type = type;
	opt_header->len = strlen(value);
	if(len == 0 || value == NULL)
		{

			log_error(logger,"illegal header in function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
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
	log_debug(logger,"Exiting function: %s\n",__func__);
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
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	char* buf = (char*)malloc(strlen(msg));
        strncpy(buf,msg,strlen(msg));
	log_info(logger,"YAML msg : %s\n",buf);
	args[0] =NULL;
        char*tkn,*temp;
        char* bufp =buf;
        int j =0,cnt =0,i=0;
	int max = strlen(buf);
        for(j =0;j<strlen(buf);j++)
        {
                if(buf[j] == '\'' || buf[j] == '\"')
                        buf[j] = ' ';
        }
nxt:
    	while(buf)
     	{
		log_debug(logger,"parsing line\n");
		cnt =0;
        	j=0;
        	while(buf[j] != '\n')
        	{        
			cnt++; j++;
			if(j == max){
				log_warn(logger,"something wrong in YAML parsing of string:\n %s\n",msg);
				return;	}
		}
	 	//read 1 line
        	char* line ;
		line =NULL;
        	line = malloc(cnt);
        	strncpy(line,buf,cnt);
		log_debug(logger,"line parsed: %s\n",line);
       	 	line[cnt]='\0';
        	char * ck;
        	if((ck = strstr(line, "method"))!=NULL)
        	{
			log_debug(logger,"YAML: Method\n");
                	//This line has method
                	tkn = strtok(line,":");
                	tkn = strtok(NULL,":,");
                	//func name
                	log_debug(logger,"Function name : %s\n",tkn);
                	tkn = trimwhitespace(tkn);
                	*func = (char*)malloc(strlen(tkn)+1);
                	strncpy(*func,tkn,strlen(tkn));	

        	}
        	else if((ck = strstr(line, "args"))!=NULL)
        	{
			log_debug(logger,"YAML: Args\n");
			/*if((ck = strstr(line,"key"))==NULL)
			{
				free(line);
				buf = buf+cnt+1;
				goto nxt;
			}*/
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
			else
			{
				free(line);
				buf = buf+cnt+1;
				goto nxt;
			}
                // tstr = " key1: 1,key2:2 " or " key1:1 "
                	char * t;
                	t= strtok(tstr,",:");//key
                	do
                	{
                                //1 key value pair
                        	t = strtok(NULL,":,%"); //value
                        	if(t ==NULL){
					buf = buf+cnt+1;
                                	goto nxt;
				}
				t = trimwhitespace(t);
                        	log_debug(logger,"arg value[%d]: %d\n",i, atoi(t));
                       		t = trimwhitespace(t);
                        	temp = (char*)malloc(strlen(t));
                        	strncpy(temp,t,strlen(t)+1);
                        	args[i] = temp;
                        	i++;
                	}while((t= strtok(NULL,",:%"))!=NULL); //key
    			args[i] =NULL;
			log_debug(logger,"Parsed all args\n");
                }

	//
	else if((ck = strstr(line, "trigger"))!=NULL)
	{
		log_debug(logger,"YAML: trigger\n");
		tkn = strtok(line,":");
                tkn = strtok(NULL,":,");
                //event name
                log_debug(logger,"Event name : %s\n",tkn);
                tkn = trimwhitespace(tkn);
                *event = (char*)malloc(strlen(tkn)+1);
                strncpy(*event,tkn,strlen(tkn));
     //           *event[strlen(tkn)] = '\0';

	}
        buf = buf+cnt+1;

    }
	
        log_debug(logger,"Exiting function: %s\n",__func__);

}




/**************************************************************************************
 * Function: MAGIMessageDecode
 * This function returns MAGIMessage structure from buffer(deserialize)
 * This function allocates memory for the structure. This should be freed by the caller.
 **************************************************************************************/
MAGIMessage_t *MAGIMessageDecode(char* msgBuf)
{

	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	/*Caller has realized it is a MAGI message*/
	MAGIMessage_t* MAGImsg = (MAGIMessage_t*) malloc(sizeof(MAGIMessage_t));
	if(MAGImsg == NULL)
	{
		log_error("malloc failed in function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
		exit(0);
	}
	MAGImsg->headers =NULL; 
	char* parser = msgBuf;
	int len_optHeaders;

	/*Now parser points to length*/
	memcpy(&MAGImsg->length,parser,4);
	MAGImsg->length = ntohl(MAGImsg->length);

	parser = parser+ 4; /*HeaderLen*/
	memcpy(&MAGImsg->headerLength,parser,2);
	MAGImsg->headerLength = ntohs(MAGImsg->headerLength);
	len_optHeaders = MAGImsg->headerLength -6; 

	parser = parser+2;/*points to Identifier*/
	memcpy(&MAGImsg->id,(void*)parser,4);
	MAGImsg->id = ntohl(MAGImsg->id);

	parser = parser + 4; /*Flags*/
	memcpy(&MAGImsg->flags,(void*)parser,1);

	parser = parser + 1; /*content type*/
	memcpy(&MAGImsg->contentType,(void*)parser,1);
	parser = parser +1;	

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

	MAGImsg->data = (char*)malloc(MAGImsg->length - MAGImsg->headerLength - 2);

	switch(MAGImsg->contentType)
	{
		case YAML:
		       		memcpy(MAGImsg->data,parser,MAGImsg->length - MAGImsg->headerLength - 2 );
				log_debug(logger,"Received YAML message...\n");	
				char* event =NULL;			
				decodeYAML(MAGImsg->data,&(MAGImsg->funcArgs.func),&(MAGImsg->funcArgs.args),&event);
				if(MAGImsg->funcArgs.func == NULL)
				  return;		
				log_debug(logger,"Calling function\n");
				char* retVal = NULL; 
				retVal = malloc(1024);
				char* retType = malloc(50); 
				call_function(MAGImsg->funcArgs.func,&(MAGImsg->funcArgs.args),retVal,retType);
			//	if(retVal != NULL)
				log_info(logger,"MAGIDecode() Call function returned\n");
				log_info(logger,"retVal of function :%s",retVal);
				if(event)
				{
				
					log_info(logger,"Sending out Trigger message\n");
					char ndata[1500];
					if(!strcmp(retType,"dictionary"))
				//	char ck[50];
				//	strncpy(ck,retVal,50);
				//	char* tk = strtok(ck,":,"); 
				//	if(tk)
					{
						
						sprintf(ndata,"{event: %s, %s, nodes: %s}",event,retVal,hostName);


					}
					else{
					 /*Todo: Malloc this strlen(event+retVal+hostName)*/
					sprintf(ndata,"{event: %s, retVal: %s, nodes: %s}",event,retVal,hostName);
					}
					free(retVal);
					log_info(logger,"Trigger message: %s\n",ndata);
					char* td = (char*)malloc(strlen(ndata));
					strcpy(td,ndata);
					trigger("control", "control", YAML, td);
					log_info(logger,"Sent the trigger message for event: %s\n",event);
				}
				break;

		default:
				log_info(logger,"Content Type not supported\n");
				break;

	}
	log_debug(logger,"Exiting function: %s\n",__func__);
	return (MAGImsg);

}


/**************************************************************************************
 * Function: MAGIMessageEncode
 * This function returns a serialized buffer from MAGIMessage structure
 * This function allocates memory for the buffer. This should be freed by the caller.
 **************************************************************************************/
char * MAGIMessageEncode(char** b,MAGIMessage_t* MAGImsg, uint32_t* bufLen)
{
	char * msgBuf;
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	msgBuf = (char*)malloc(MAGImsg->length+4);
	/*err check*/
	char* tmp = msgBuf;

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
	memcpy((void*)tmp,(void*)MAGImsg->data, MAGImsg->length - MAGImsg->headerLength - 2 );
	*bufLen = MAGImsg->length+4;


	log_debug(logger,"Exiting function: %s\n",__func__);
	*b = msgBuf;
	return msgBuf;

}

