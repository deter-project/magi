
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *agentName,*dockName,*logFileName=NULL,*commGroup=NULL,*commHost=NULL,*hostName =NULL;
int log_level,commPort =0;
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
void main(int argc, char**argv)
{
	agentName = (char*)malloc(strlen(argv[1]+1));
	strcpy(agentName,argv[1]);
	dockName = (char*)malloc(strlen(argv[2]+1));
	strcpy(dockName,argv[2]);

	int count = 3;
	while(count < argc)
	{
		char* temp = (char*)malloc(strlen(argv[count])+1);
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

