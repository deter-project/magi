#include <stdio.h>
#include<stdlib.h>
#include <stdarg.h>
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

void add(int a, char* b)
{
	printf(" a :%d\n b:%s\n",a,b);
	return;
}

void divi(int c,int d, int e )
{

	printf("c d e: %d %d %d \n",c,d,e);
	return;
}

int main()
{

	addFunc("add",&add,2,"int","char*");
	//addFunc("divi",&divi);
	create_functionMap();
	char* args[4] = {"10","12",NULL};
	call_function("add",args);
}











