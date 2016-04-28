#include "Util.h"
#include "Logger.h"

#include <ctype.h>
#include <string.h>
#include <yaml.h>

extern Logger* logger;

void entrylog(Logger* log, const char* func, char* file, int line) {
	log_debug(log, "Entering function: %s in file %s", func, file);
}

void exitlog(Logger* log, const char* func, char* file, int line) {
	log_debug(log, "Exiting function: %s in file %s", func, file);
}

char* trimwhitespace(char *str) {
	char *end;

	// Trim leading space
	while (isspace(*str))
		str++;

	if (*str == 0)
		return str;

	// Trim trailing space
	end = str + strlen(str) - 1;
	while (end > str && isspace(*end))
		end--;

	// Write new null terminator
	*(end + 1) = 0;
	return str;
}

void addList(list_t** list, char* data) {
	list_t* temp = (list_t*) malloc(sizeof(list_t));
	temp->data = malloc(strlen(data) + 1);
	strcpy(temp->data, data);
	temp->next = *list;
	*list = temp;
}

// Utility function to insert a node in a link list.
dictionary insert(dictionary* list, char* key, char* value) {
	dList_t* head = *list;
	dList_t* temp = malloc(sizeof(dList_t));
	if (temp == NULL)
		return NULL;
	temp->name = malloc(strlen(key) + 1);
	strcpy(temp->name, key);

	temp->value = malloc(strlen(value) + 1);
	strcpy(temp->value, value);

	if (head == NULL) {
		temp->next = NULL;
		head = temp;
		*list = head;
		return head;
	}
	temp->next = head;
	head = temp;
	*list = head;
	return head;
}

// Utility function to find a key in a link list.
char* dfind(dList_t* list, char* key) {
	dList_t* temp = list;
	if (list == NULL)
		return NULL;

	while (temp != NULL) {
		if (!(strcmp(temp->name, key))) {
			return (temp->value);
		}
		temp = temp->next;
	}
	return NULL;
}

/**
 * Converts a dictionary to a yaml encoded string.
 *
 * @param d - dictionary object to be encoded.
 */
char* dictToYaml(dictionary d) {
	dictionary temp = d;
	int len = 0;
	if (d == NULL) {
		return NULL;
	}

	while (temp != NULL) {
		len += strlen(temp->name);
		len += strlen(temp->value);
		len += 3;
		temp = temp->next;
	}
	temp = d;
	char* dBuf = malloc(len);

	memset(dBuf, 0, len);
	int i = 0;
	while (temp != NULL) {
		if (i == 0) {
			strcpy(dBuf, temp->name);
		} else {
			strcat(dBuf, " ");
			strcat(dBuf, temp->name);
		}
		strcat(dBuf, ": ");
		strcat(dBuf, temp->value);
		if (temp->next != NULL)
			strcat(dBuf, ",");
		temp = temp->next;
		i++;
	}

	return dBuf;
}

char* parseYamlString(char* input){
	char* retString = "undefined";

	yaml_parser_t parser;
	yaml_parser_initialize(&parser);

	size_t length = strlen(input);
	yaml_parser_set_input_string(&parser, input, length);

	yaml_token_t token;
	do {
		yaml_parser_scan(&parser, &token);

		if(token.type == YAML_SCALAR_TOKEN) {
			//printf("scalar %s \n", token.data.scalar.value);
			retString = malloc(strlen(token.data.scalar.value) + 1);
			strcpy(retString, token.data.scalar.value);
			break;
		}

		if(token.type != YAML_STREAM_END_TOKEN){
			yaml_token_delete(&token);
		}

	} while (token.type != YAML_STREAM_END_TOKEN);
	yaml_token_delete(&token);

	/* Cleanup */
	yaml_parser_delete(&parser);

	return retString;
}

void freeList(list_t* list){
	entrylog(logger, __func__, __FILE__, __LINE__);
	list_t* node;
	while(list != NULL){
		node = list;
		list = node->next;
		free(node->data);
		free(node);
	}
	exitlog(logger, __func__, __FILE__, __LINE__);
}
