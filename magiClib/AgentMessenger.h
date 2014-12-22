
#include "logger.h"
/*This structure holds dictionary data structure and also class variables*/
typedef struct dList_s
{
	char* name;
	char* value;
	struct dList_s* next;
}dList_t;

typedef dList_t* dictionary;


