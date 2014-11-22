#include "AgentTransport.h"
#include "logger.h"
#include <netdb.h>


int portno;
char msg[4096];

pthread_t sender,listener;


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
	printf("Queue NULL, Enqueing\n");
        transport->rear = (Queue_t *)malloc(sizeof(Queue_t));
        transport->rear->next= NULL;
        transport->rear->req=req;
	transport->rear->req->data = req->data;
        transport->front = transport->rear;

    }
    else
    {
	printf("Queue not NULL; Enqueing at rear\n");
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
		printf("Dequeue: Got an element on the queue\n");
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
	printf("Entering sendOut\n");
	enqueue(outTransport,req);
	printf("Exiting sendOut\n");

}
/***************************************************************
Send Thread
**************************************************************/
void* sendThd()
{
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
			printf("SndThd: Sending out req type:%d\n",req->reqType);
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

}

/***************************************************************
Listen Thread
**************************************************************/

void* listenThd()
{
	while(1)
	{
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
		printf("ListenThd: Received an AgentRequest message\n");
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
		//printf("Got a message on listen queue\n");
		AgentRequest_t* req = AgentDecode(msg);
		printf("ListenThd: decode complete...enqueing\n"); 	
		inTransport = enqueue(inTransport,req);
		
		/*Notify next() if non-blocking*/

	}

}

/***************************************************************
Parse helper
**************************************************************/
void parse_args(int argc, char**argv)
{
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
			printf("Malloc failed: Parsing arguments\n");
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



}



/************************************************************
* Parses all the incoming args and sets values
*
**************************************************************/
void init_connection(int argc,char** argv)
{

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
		printf("error creating log file\n");
	logger = Logger_create(logFile,log_level);
        if(logger==NULL)
                printf("error creating logger\n");
	
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

}

/*****************************************************************
	main thd
******************************************************************/
void start_connection()
{
	//Connect to the given socket
	if (connect(fd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
	{
		perror("Connection to daemon failed: ");
		exit(0);
	}
	log_info(logger,"Agent connected to daemon\n");

	/*Start Sender Thd*/
	int err = pthread_create(&sender,NULL,&sendThd,NULL);
	if(err < 0)
	{
		printf("Error in creating sender\n");
		exit(0);
	}
	/*Start Listen Thd*/
	err = pthread_create(&listener,NULL,&listenThd,NULL);
	if(err < 0)
	{
		printf("Error in creating listener\n");
		exit(0);
	}
	log_info(logger,"Agent Sender and Listener threads active\n");
	
	log_info(logger,"Sending a Listen Dock Msg\n");
	listenDock(dockName);


}

/*************************************
 *
 ************************************* */

void stop_connection()
{

//close socket


}
