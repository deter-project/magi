#ifndef _UTIL_H
#define _UTIL_H

#include "Logger.h"

void entrylog(Logger *log, const char* func, char* file, int line);
void exitlog(Logger *log, const char* func, char* file, int line);

char* trimwhitespace(char *str);

typedef struct list_s {
	char* data;
	struct list_s* next;
} list_t;

typedef struct dList_s
{
	char* name;
	char* value;
	struct dList_s* next;
}dList_t;

typedef dList_t* dictionary;

void addList(list_t** list, char* data);
dictionary insert(dictionary* list, char* key, char* value);
char* dfind(dList_t* list, char* key);
char* dictToYaml(dictionary d);
char* parseYamlString(char* input);

void freeList(list_t* list);

#endif /* _UTIL_H */
