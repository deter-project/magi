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
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "AgentMessenger.h"
dList_t* list;
int* add(int a, int b) 
{
	int* result = (int*)malloc(sizeof(int));
//	char* value = dfind(list,"delta"); //class variable eq
//	if(value != NULL)
//        {	
//		int temp = atoi(value);
		*result = a+b;
		return result;
//	}
//	else
 //		return a+b;	
}


char* ccat(char* a, char*b)
{
 	
	char* result = malloc(strlen(a)+strlen(b)+1);
  	strcpy(result,a);
	strcat(result,b);
	return result;	 

}


int* sub(int a, int b)
{

	int* result = malloc(sizeof(int));
	*result = a-b;
	return result;	

}

dictionary dictTest(char* a, char* b)
{

	dictionary d=NULL;
	insert(&d,"arg1",a);
	insert(&d,"arg2",b);
	return d;
}	

int main(int argc, char **argv)
{
	addFunc("add","int*",&add,2,"int","int");
	addFunc("sub","int*",&sub,2,"int","int");
	addFunc("ccat","char*",&ccat,2,"char*","char*");
	addFunc("dictTest","dictionary",&dictTest,2,"char*","char*");
	list = ArgParser(argc,argv);
	agentStart(argc,argv);
}
