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
int add(int a, int b) 
{
	printf("Function add\n");
	printf("a: %d\nb:%d\n", a,b);
	return a+b;	
}

float add2(int a, int b,int c) 
{ 
	printf("Function add2\n");
	printf("a: %d\nb:%d\nc:%d\n", a,b,c);
	return a+b+c;	
	
}

int cpyfunc(char* a)
{
	printf("cpyFunc: %s\n",a);
	return 1;

}

int main(int argc, char **argv)
{
	addFunc("add",&add,2,"int","int");
	//addFunc("add2",&add2,3,"int","int");
	//addFunc("cpyfunc",&cpyfunc,1,"char*");
	agentStart(argc,argv);
}
