#include "AgentTransport.h"
#include "logger.h"
#include <netdb.h>
#include <pthread.h>

int portno;
char msg[4096];

pthread_t sender,listener;

static int listener_stop = 0,listener_clear =0; 
static int fd;
Transport_t* inTransport,*outTransport;
struct sockaddr_in serv_addr;/*Holds Address info of server*/
struct hostent *server;

FILE* logFile;
Logger* logger;

char *agentName,*dockName,*logFileName=NULL,*commGroup=NULL,*commHost=NULL,*hostName =NULL;
int log_level,commPort =0;



/******************************************************************
 Enqueing
******************************************************************** */
Transport_t * enqueue(Transport_t *transport,AgentRequest_t *req)
{
	/*Add new nodes at rear*/
	pthread_mutex_lock(&transport->qlock);
    if (transport->rear == NULL)
    {
	log_debug(logger,"Queue NULL, Enqueing...\n");
        transport->rear = (Queue_t *)malloc(sizeof(Queue_t));
        transport->rear->next= NULL;
        transport->rear->req=req;
	transport->rear->req->data = req->data;
        transport->front = transport->rear;

    }
    else
    {
	log_debug(logger,"Queue not NULL; Enqueing at rear\n");
        Queue_t* temp=(Queue_t *)malloc(sizeof(Queue_t));
	temp->req = req;
	temp->next = NULL;
        transport->rear->next = temp;
	transport->rear = temp;
    }
    	pthread_mutex_unlock(&transport->qlock);
	return transport;
}
 
/*****************************************************************
 Dequeing 
******************************************************************/
AgentRequest_t* dequeue(Transport_t *transport)
{
	pthread_mutex_lock(&transport->qlock);
    	Queue_t* front = transport->front;
    	if (front == NULL)
   	{
	   /*No elements in  the queue*/
		pthread_mutex_unlock(&transport->qlock);
        	return NULL;
    	}
   	else
   	{
		log_debug(logger,"Dequeue: Got an element on the queue\n");
	    	AgentRequest_t* node;
    		node =	front->req;  
	    	transport->front = front->next; 
	    	//free(front)
		if(transport->front == NULL)
	    	{
			transport->rear = NULL;
	    	}
		pthread_mutex_unlock(&transport->qlock);
		return node;
	}
	    
}

int isEmpty(Transport_t* transport)
{
    return (transport->front == NULL);
}


/*************************************
 *
 ************************************* */
void sendOut(AgentRequest_t* req)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	enqueue(outTransport,req);
	log_debug(logger,"Exiting function: %s\n",__func__);

}
/***************************************************************
Send Thread
**************************************************************/
void* sendThd()
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	AgentRequest_t* req = NULL;
	while(1)
	{
		req= dequeue(outTransport);
       		if(req!=NULL)
       		{
			log_info(logger,"dequeueing outTransport message...\n");
			/*Msg in the queue to be sent out*/
			int length =0;
			char* msg;
			msg = AgentEncode(req,&length);
			log_info(logger,"Sending out msg on socket...\n");
			int err = send(fd, msg, length, 0);
			if(err == -1)
			{
				log_error(outTransport->logger,"Message send failed...\n%s:%d:%s\n",__FILE__, __LINE__,__func__);

			}
			req = NULL;
       		}

	}
	log_debug(logger,"Exiting function: %s\n",__func__);

}

/***************************************************************
Listen Thread
**************************************************************/

void* listenThd()
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	while(!listener_stop)
	{
		/*pthread_Cancel should exit from read blocking call. If a bug appears, then
		read has to be timed*/

		int len = read(fd,msg,sizeof(msg)); 
		if(len < 8) continue;
		char magi[8]; 
		memcpy(magi,msg,8);
		if(strncmp(magi,"MAGI\x88MSG",8))
		{
			log_info(logger,"Received invalid Agent msg...\n");
			continue;
		}
		log_info(logger,"Received a AgentRequest message...\n");
		log_debug(logger,"ListenThd: Received an AgentRequest message\n");
		char* ptr = msg;
		ptr=ptr+8;
		uint32_t totalLen;
		memcpy(&totalLen,ptr,4);
		totalLen = ntohl(totalLen);
		char tmp[4096];
		int len_t = len;
	 	while(len_t < totalLen)
		{
			int len1 = read(fd,tmp,(totalLen -len_t));
			if(len1 <= 0) continue;
			memcpy(msg+len_t,tmp,len1);
			len_t +=len1;
		}

		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
		listener_clear = 0;
		AgentRequest_t* req = AgentDecode(msg);
	
		inTransport = enqueue(inTransport,req);
		listener_clear = 1;
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
		pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);		
		/*Notify next() if non-blocking*/

	}
	log_debug(logger,"Exiting function: %s\n",__func__);

}

/***************************************************************
Parse helper
**************************************************************/
void parse_args(int argc, char**argv)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	agentName = (char*)malloc(strlen(argv[1]+1));
	strcpy(agentName,argv[1]);
	dockName = (char*)malloc(strlen(argv[2]+1));
	strcpy(dockName,argv[2]);

	int count = 3;
	while(count < argc)
	{
		char* temp;
		temp = (char*)malloc(strlen(argv[count])+1);
		if(temp ==NULL)
		{
			log_error(logger,"Malloc failed: Parsing arguments\n");
			exit(0);
		}
		strcpy(temp,argv[count]);
		char* tkn = strtok(temp,"=");
		tkn = trimwhitespace(tkn);
		if(!strcmp(tkn,"commGroup"))
		{
			tkn = strtok(NULL,"=");
			tkn = trimwhitespace(tkn);
			commGroup = (char*) malloc(strlen(tkn)+1);
			strcpy(commGroup,tkn);
		}

		else if(!strcmp(tkn,"loglevel"))
		{
			tkn = strtok(NULL,"=");
			tkn = trimwhitespace(tkn);
			log_level = atoi(tkn);
			if(log_level < 0 || log_level > 3)
				log_level = 0;

		}
		else if(!strcmp(tkn,"logfile"))
		{
			tkn = strtok(NULL,"=");
			tkn = trimwhitespace(tkn);
			logFileName = (char*) malloc(strlen(tkn)+1);
			strcpy(logFileName,tkn);

		}
		else if(!strcmp(tkn,"commHost"))
		{
			tkn = strtok(NULL,"=");
			tkn = trimwhitespace(tkn);
			commHost = (char*) malloc(strlen(tkn)+1);
			strcpy(commHost,tkn);

		}
		else if(!strcmp(tkn,"commPort"))
		{
			tkn = strtok(NULL,"=");
			tkn = trimwhitespace(tkn);
			commPort = atoi(tkn);

		}
		else if(!strcmp(tkn,"hostname"))
		{
			tkn = strtok(NULL,"=");
			tkn = trimwhitespace(tkn);
			hostName = (char*) malloc(strlen(tkn)+1);
			strcpy(hostName,tkn);

		}
		else if(!strcmp(tkn,"execute"))
		{

		}
		free(temp);
		temp=NULL;
		count++;

	}

	log_debug(logger,"Exiting function: %s\n",__func__);


}



/************************************************************
* Parses all the incoming args and sets values
*
**************************************************************/
void init_connection(int argc,char** argv)
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	parse_args(argc,argv);
	/*Create logger and start logging*/
	if(logFileName == NULL)
	{
		logFileName = (char*)malloc(strlen(agentName)+strlen(".log")+1);
		strcpy(logFileName,agentName);
		strcat(logFileName,".log");
	}
	logFile = fopen(logFileName,"w");	
	if(logFile == NULL)
		log_error(logger,"error creating log file\n");
	logger = Logger_create(logFile,log_level);
        if(logger==NULL)
                log_error(logger,"error creating logger\n");
	
	/*Set default values for fields with no value*/
	if(agentName == NULL)
		agentName = "cAgent"; /*Return error if this field is mandatory*/
	if(dockName == NULL)
		dockName = "cAgent_dock";
	if(commHost == NULL)
		commHost = "localhost";
	if(hostName == NULL)
		hostName = "MAGIdaemon";
	if(commPort == 0)
		commPort = 18809;	

	/*Log the parsed info*/
	
	log_info(logger,"agentName : %s\ndockName : %s\ncommHost : %s\nhostName : %s\nlogFileName : %s\ncommPort : %d\nloglevel : %d\n", agentName,dockName,commHost,hostName,logFileName,commPort,log_level);
	if(commGroup != NULL)
	{
		log_info(logger,"commGroup : %s\n", commGroup);
		joinGroup(commGroup);
	}


	/*Set up transport queues*/
	inTransport = (Transport_t*)malloc(sizeof(Transport_t));
	outTransport = (Transport_t*)malloc(sizeof(Transport_t));
	pthread_mutex_init(&inTransport->qlock, NULL);
	pthread_mutex_init(&outTransport->qlock, NULL);
	inTransport->front = inTransport->rear = NULL;
	outTransport->front =outTransport->rear = NULL;
	/*init socket*/	
	fd = socket(AF_INET,SOCK_STREAM,0);
   	if (fd < 0) 
	{
		perror("Socket creation failed: ");
        	exit(0);
	}

	/*Set addr and port*/
	server = gethostbyname(commHost);
	portno = commPort;

   	bzero(&serv_addr,sizeof(serv_addr));
   	serv_addr.sin_family = AF_INET;

    	bcopy((char *)server->h_addr,(char *)&serv_addr.sin_addr.s_addr,server->h_length);	
	//serv_addr.sin_addr.s_addr=(inet_addr("127.0.0.1"));
	serv_addr.sin_port = htons(portno);
	log_debug(logger,"Exiting function: %s\n",__func__);

}

/*****************************************************************
	main thd
******************************************************************/
void start_connection()
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	//Connect to the given socket
	if (connect(fd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
	{
		log_error(logger,"Connection to daemon failed: ");
		exit(0);
	}
	log_info(logger,"Agent connected to daemon\n");

	/*Start Sender Thd*/
	int err = pthread_create(&sender,NULL,&sendThd,NULL);
	if(err < 0)
	{
		log_error(logger,"Error in creating sender\n");
		exit(0);
	}
	/*Start Listen Thd*/
	err = pthread_create(&listener,NULL,&listenThd,NULL);
	if(err < 0)
	{
		log_error(logger,"Error in creating listener\n");
		exit(0);
	}
	log_info(logger,"Agent Sender and Listener threads active\n");
	
	log_info(logger,"Sending a Listen Dock Msg\n");
	listenDock(dockName);
	log_debug(logger,"Exiting function: %s\n",__func__);

}

void closeTransport()
{
	log_debug(logger,"Entering function: %s\n\t in File \"%s\", line %d \n",__func__,__FILE__,__LINE__);
	pthread_cancel(listener);
	listener_stop = 1;
	while(!(outTransport->front ==NULL && listener_clear));
	//ready to close send thd and close the socket
	pthread_cancel(sender);
	close(fd);
	log_debug(logger,"Exiting function: %s\n",__func__);

}
