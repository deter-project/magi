#include "AgentRequest.h"
#include "MAGIMessage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include "logger.h"

extern fargs_t funcArgs;
char* nodeName;
int add(int a, char*b) 
{
	printf("Function add\n");
	printf("a: %d\nb:%s\n", a,b);
	return a;	
}

float divide(char**args) 
{ 
	int i=0;
	printf("Function divide\n");
	printf("res: %f\n",(float)atoi(args[0])/atoi(args[1]) );
	return (atoi(args[1])/atoi(args[2]));	
	

}


int main(int argc, char **argv)
{

	init_connection(argc,argv);
	start_connection();
	addFunc("add",&add,2,"int","char*");
//	addFunc("divide")
	create_functionMap();
	send_start_trigger();


//	int block =1;
//	MAGIMessage_t* in;
	while(1)
{
		sleep(1);
	//	in= next(block);
	//	if(in)
 			//call_function((funcArgs.func),&(funcArgs.args));
	}
}
