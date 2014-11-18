#include "AgentTransport.h"
extern Transport_t* inTransport,*outTransport;
extern Logger* logger;

extern char *agentName,*dockName,*logFileName,*commGroup,*commHost,*hostName;
extern int log_level,commPort;
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
	AgentRequest_t* req = (AgentRequest_t*)malloc(sizeof(AgentRequest_t));
	req->reqType = reqType;
	req->options = NULL;
	req->data = data;
	printf("CreateAgentReq data: %s\n%s\n",data,req->data);
	return req;
}

/*************************************
 *
 ************************************* */
void listenDock(char* dock)
{
	//printf("Inside listenDock\n");
	AgentRequest_t* req = createAgentRequest(LISTEN_DOCK,dock);
	//printf("Created req\n");
	sendOut(req);
}

/*************************************
 *
 ************************************* */
void unlistenDock(char* dock)
{
	AgentRequest_t* req = createAgentRequest(UNLISTEN_DOCK,dock);
	sendOut(req);
}
/*************************************
 *
 ************************************* */
void joinGroup(char* group)
{
	AgentRequest_t* req = createAgentRequest(JOIN_GROUP,group);
	sendOut(req);
}
/*************************************
 *
 ************************************* */
void leaveGroup(char* group)
{
	AgentRequest_t* req = createAgentRequest(LEAVE_GROUP,group);
	sendOut(req);
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
		MAGIMessage_t* magiMsg = (MAGIMessage_t*)malloc(sizeof(MAGIMessage_t));
		if(srcdock)
			insert_header(magiMsg, SRCDOCK, strlen(srcdock)+1, srcdock);
		if(node)
			insert_header(magiMsg, DSTNODES, strlen(node)+1, srcdock);
		if(group)
			insert_header(magiMsg, DSTGROUPS, strlen(group)+1, srcdock);
		if(dstdock)
			insert_header(magiMsg, DSTNODES, strlen(dstdock)+1, srcdock);
		magiMsg->contentType = contenttype;
		magiMsg->data = data;

		magiMsg->flags = 0;
		magiMsg->headerLength = calHlen(magiMsg->headers);
		magiMsg->id = 0; 
		magiMsg->length= 2+magiMsg->headerLength+strlen(data)+1;
		return magiMsg;
	}



/*************************************
 *
 ************************************* */
/*AgentRequest header options - end with NULL*/
void MAGIMessageSend(MAGIMessage_t* magi,char* arg,...)
{
	
	AgentRequest_t* req = createAgentRequest(MESSAGE,magi);
	req->data = (char*)magi;
	MAGIMessage_t* t = req->data;
	printf("MAGIMSGSend : %s/n",t->data);
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
	printf("MAGI sendMsg calling sendOut\n");
	int len =0;
	//AgentEncode(req,&len);
	//printf("MAGI msg encode chk success.. Calling sendOut()\n");
	sendOut(req);
}

void trigger(char* groups, char* docks, contentType_t contenttype, char* data)
{

	MAGIMessage_t* msg = create_MAGIMessage(NULL, NULL, groups, docks, contenttype, data);
	printf("trigger:Created MAGI msg for trigger %s\n",msg->data);
	MAGIMessageSend(msg,NULL);
	//printf("trigger_gen complete\n");

}

void send_start_trigger()
{
	char data[100]; 
	sprintf(data,"nodes:%s,event:AgentLoadDone,agent:%s",hostName,agentName);
	//printf("trigger msg -> %s\n",data);
	trigger(NULL,NULL,MESSAGE,data);
	printf("start_trigger complete\n");
}

typedef struct {
  char *name;
  int aCnt;
  char* argList[10];
  void (*func)();  
}fMap;

fMap* function_map;

typedef struct fList{
	char* name;
	void* fptr;
	int aCnt;
	struct fList* next;
	char* argList[10];

}fList_t;

int count=0;
static fList_t* funcList=NULL; 

fList_t* addFunc(char* name, void* fptr,int number,...)
{

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
	count=0;
	fList_t *temp = funcList;
	while(temp)
	{
		count++;
		temp = temp->next;

	}  

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

}  

union Data
{
	int i;
	char* s;
};


int call_function(const char *name, char** args)
{
  printf("In calling function...searching for function : %s\n",name);
  int i=0,argcnt = 0;
	int retVal = 0;
  while(args[argcnt]!=NULL)
  {
	argcnt++; /*Number of args*/		
  }
union Data data[10]; /*10 args max*/

  for (i = 0; i < count; i++)
  {
    if (!strcmp(function_map[i].name, name) && function_map[i].func) 
	{
		printf("Found function :%s\n",name);
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
			j++;

		}
      		if(argcnt ==1)
			function_map[i].func((sizeof(data[0])==sizeof(int)) ? data[0].i : data[0].s);
		else if(argcnt == 2)
			function_map[i].func((sizeof(data[0])==sizeof(int)) ? data[0].i : data[0].s,(sizeof(data[1])==sizeof(int)) ? data[1].i : data[1].s);
		else if(argcnt == 3)
			function_map[i].func(atoi(args[0]),atoi(args[1]),atoi(args[2]));
		printf("Done\n");
		return retVal;
    	}
  }

  return -1;
}
