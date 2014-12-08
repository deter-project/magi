#include "AgentTransport.h"
extern Transport_t* inTransport,*outTransport;
extern Logger* logger;
int stop_flag = 0;
extern char *agentName,*dockName,*logFileName,*commGroup,*commHost,*hostName;
extern int log_level,commPort;
union Data
{
	int i;
	char* s;
};
union Data data[10]; /*10 args max*/
#define ARG(x) (sizeof(data[x])==sizeof(int)) ? data[x].i : data[x].s

/*************************************
 *
 ************************************* */

MAGIMessage_t* next(int block)
{
	AgentRequest_t* req;
	if(block)
	{
		//replace spin by notify
		while((req = dequeue(inTransport))==NULL)
		{
			//block
		}

	}
	else
		req = dequeue(inTransport);
	
	if(!req) return NULL;
	if(req->reqType == MESSAGE)
	{
		return (MAGIMessage_t*) req->data;	
	}
	else
	{
		log_info(logger,"Non-MAGI message received\n");
		return NULL;
	}
}

/*************************************
 *
 ************************************* */
AgentRequest_t* createAgentRequest(AgentRequestType_t reqType,char* data)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = (AgentRequest_t*)malloc(sizeof(AgentRequest_t));
	req->reqType = reqType;
	req->options = NULL;
	req->data = data;
	log_debug(logger,"Exiting function: %s\n",__func__);	
	return req;
}

/*************************************
 *
 ************************************* */
void listenDock(char* dock)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = createAgentRequest(LISTEN_DOCK,dock);
	log_debug(logger,"Exiting function: %s\n",__func__);
	sendOut(req);
}

/*************************************
 *
 ************************************* */
void unlistenDock(char* dock)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = createAgentRequest(UNLISTEN_DOCK,dock);
	sendOut(req);
	log_debug(logger,"Exiting function: %s\n",__func__);

}
/*************************************
 *
 ************************************* */
void joinGroup(char* group)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = createAgentRequest(JOIN_GROUP,group);
	sendOut(req);
	log_debug(logger,"Exiting function: %s\n",__func__);

}
/*************************************
 *
 ************************************* */
void leaveGroup(char* group)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = createAgentRequest(LEAVE_GROUP,group);
	sendOut(req);
	log_debug(logger,"Exiting function: %s\n",__func__);

}



/**
* Create a partially filled MAGI message
	 * @param srcdock if not null, the message srcdock is set to this
	 * @param node if not null, node is added to the list of destination nodes
	 * @param group if not null, group is added to the list of destination groups
	 * @param dstdock if not null, dstdock is add to the list of destination docks
	 * @param contenttype the contenttype for the data as specified in the Messenger interface
	 * @param data encoded data for the message or null for none.
	 */
MAGIMessage_t* create_MAGIMessage(char* srcdock, char* node, char* group, char* dstdock, contentType_t contenttype, char* data)
	{
		log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
		MAGIMessage_t* magiMsg = (MAGIMessage_t*)malloc(sizeof(MAGIMessage_t));
		if(srcdock)
			insert_header(magiMsg, SRCDOCK, strlen(srcdock)+1, srcdock);
		if(node)
			insert_header(magiMsg, DSTNODES, strlen(node)+1, node);
		if(group)
			insert_header(magiMsg, DSTGROUPS, strlen(group)+1, group);
		if(dstdock)
			insert_header(magiMsg, DSTNODES, strlen(dstdock)+1, dstdock);
		magiMsg->contentType = contenttype;
		magiMsg->data = data;

		magiMsg->flags = 0;
		magiMsg->headerLength = calHlen(magiMsg->headers)+6;
		magiMsg->id = 0; 
		magiMsg->length= 2+magiMsg->headerLength+strlen(data);
		log_debug(logger,"Exiting function: %s\n",__func__);

		return magiMsg;
	}



/*************************************
 *
 ************************************* */
/*AgentRequest header options - end with NULL*/
void MAGIMessageSend(MAGIMessage_t* magi,char* arg,...)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = createAgentRequest(MESSAGE,magi);
	req->data = (char*)magi;
	MAGIMessage_t* t = req->data;
	
	if(arg !=NULL)
	{
		va_list kw;
    		va_start(kw, arg);
		char* args=arg;
		char* key,*value,*tok;
		char* str;
		
		do{
			str = (char*)malloc(strlen(args));
		        strncpy(str,args,strlen(args));
     			tok = strtok(str,"=");
			key = (char*)malloc(strlen(tok));
			strncpy(key,tok,strlen(tok));
			if(key == NULL){
				printf("Invalid option\n");
				free(key);
				continue;
			}
			tok = strtok(NULL,"=");
			value = (char*)malloc(strlen(tok));
			strncpy(value,tok,strlen(tok));

			if(value == NULL)
			{
				printf("Invalid option\n");
				free(key);
				free(value);
				continue;

			}

			add_options(&req->options,key,strlen(value),value);
			free(str);
			free(key);

		}while((args = va_arg(kw, char*)) != NULL);
   	 	va_end(arg);
	}
	log_debug(logger,"Exiting function: %s\n",__func__);
	sendOut(req);
}

void trigger(char* groups, char* docks, contentType_t contenttype, char* data)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	MAGIMessage_t* msg = create_MAGIMessage(NULL, NULL, groups, docks, contenttype, data);
	log_debug(logger,"trigger:Created MAGI msg for trigger %s\n",msg->data);
	MAGIMessageSend(msg,NULL);
	log_debug(logger,"Exiting function: %s\n",__func__);

}

void send_start_trigger()
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	char* data = (char*)malloc(100);
	char temp[100]; 
	sprintf(temp,"{nodes: %s, event: AgentLoadDone, agent: %s}",hostName,agentName);
	strcpy(data,temp);
	trigger("control",NULL,MESSAGE,data);
	log_debug(logger,"Exiting function: %s\n",__func__);
}

typedef struct {
	char *name;
	int aCnt;
  	char* argList[10];
  	int (*func)();  
}fMap;

fMap* function_map;

typedef struct fList{
	char* name;
	int* fptr;
	int aCnt;
	struct fList* next;
	char* argList[10];

}fList_t;

int count=0;
static fList_t* funcList=NULL; 

fList_t* addFunc(char* name, void* fptr,int number,...)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	fList_t* tmp = (fList_t*)malloc(sizeof(funcList));
	tmp->name = malloc(strlen(name)+1);

	va_list kw;
    	va_start(kw, number);
	char* args;
	int k;
	k =0;
	while((args = va_arg(kw,char*)) != NULL && k < number)
	{
		tmp->argList[k] = (char*)malloc(strlen(args)+1);
		strcpy(tmp->argList[k],args);
		k++;			
	}
	tmp->aCnt = k;
	tmp->name = name; /*Fix*/
	tmp->fptr = fptr;
	if(funcList == NULL)
	{
		funcList = tmp;
		funcList->next =NULL;
	}
	else
	{
		tmp->next = funcList;
		funcList = tmp;

	}
	//va_end(args);
	return funcList;
} 


fMap* create_functionMap()
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	count=0;
	fList_t *temp = funcList;
	while(temp)
	{
		count++;
		temp = temp->next;

	}  
	if(count == 0)
		return NULL;

	function_map = malloc(count*sizeof(fMap));
	int i = 0; 
	temp = funcList;
	int cnt = count;
	while(cnt || temp)
	{
		(function_map[i]).name = temp->name;
		(function_map[i]).func = temp->fptr;
		int j =0;
		while(j<temp->aCnt)
		{
			(function_map[i]).argList[j] = temp->argList[j];		
			j++;
		}
		(function_map[i]).aCnt = temp->aCnt;
		temp=temp->next;
		cnt--;  
		i++;
	}
	return function_map;

}  

void stopFunc()
{
	stop_flag = 1;
	return;
}


int agentStart(int argc, char** argv)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	addFunc("stop",&stopFunc,0,NULL);
	fMap* t;
	if((t= create_functionMap())==NULL)
		return -1;
	init_connection(argc,argv);
	start_connection();
	send_start_trigger();
	while(!stop_flag)
		sleep(1);
	log_info(logger,"Agent closing connection...");
	// close transport module
	closeTransport();	
	
}



int call_function(const char *name, char** args)
{
  log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
  int i=0,argcnt = 0;
	int retVal = 0;
  while(args[argcnt]!=NULL)
  {
	argcnt++; /*Number of args*/		
  }
 
  for (i = 0; i < count; i++)
  {
    if (!strcmp(function_map[i].name, name) && function_map[i].func) 
	{
		log_info(logger,"Found function :%s\n",name);
		if(argcnt != function_map[i].aCnt)
			return -1;
		int j =0;
		while(j<argcnt)
		{
			if(!strcmp(function_map[i].argList[j],"int"))
				data[j].i=atoi(args[j]);
			else if(!strcmp(function_map[i].argList[j],"char*")) 
			{
				data[j].s = malloc(strlen(args[j])+1);
				strcpy(data[j].s,args[j]);	
			}
			else
			{

				//handling unknown data types

			}	
			j++;

		}
		if(argcnt == 0)
			retVal = function_map[i].func();
      		else if(argcnt ==1)
			retVal = function_map[i].func((sizeof(data[0])==sizeof(int)) ? data[0].i : data[0].s);
		else if(argcnt == 2)
			retVal = function_map[i].func((sizeof(data[0])==sizeof(int)) ? data[0].i : data[0].s,(sizeof(data[1])==sizeof(int)) ? data[1].i : data[1].s);
		else if(argcnt == 3)
			retVal = function_map[i].func((sizeof(data[0])==sizeof(int)) ? data[0].i : data[0].s,(sizeof(data[1])==sizeof(int)) ? data[1].i : data[1].s,(sizeof(data[2])==sizeof(int)) ? data[2].i : data[2].s);
		else if(argcnt == 4)
			retVal = function_map[i].func(ARG(0),ARG(1),ARG(2),ARG(3));
		else if(argcnt == 5)
			retVal = function_map[i].func(ARG(0),ARG(1),ARG(2),ARG(3),ARG(4));
		else if(argcnt == 6)
			retVal = function_map[i].func(ARG(0),ARG(1),ARG(2),ARG(3),ARG(4),ARG(5));
		else if(argcnt == 7)
			retVal = function_map[i].func(ARG(0),ARG(1),ARG(2),ARG(3),ARG(4),ARG(5),ARG(6));
		else if(argcnt == 8)
			retVal = function_map[i].func(ARG(0),ARG(1),ARG(2),ARG(3),ARG(4),ARG(5),ARG(6),ARG(7));
		else if(argcnt == 9)
			retVal = function_map[i].func(ARG(0),ARG(1),ARG(2),ARG(3),ARG(4),ARG(5),ARG(6),ARG(7),ARG(8));

		else 
		{
			log_info(logger,"Not able to handle these many number of arguments\n");
			return -1;
		}
		log_debug("Exiting %s \n",__func__);
		return retVal;
    	}
  }

  return -1;
}
