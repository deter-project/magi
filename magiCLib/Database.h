/**
 * Header file for the Database.c file.
 */

#ifndef _DATABASE_H
#define _DATABASE_H

#include <assert.h>
#include <bcon.h>
#include <mongoc.h>
#include <stdio.h>

// Operation types supported by the MAGI C library for MongoDB Operations.
typedef enum{
        OPER_INSERT = 1,
        OPER_FIND = 2,
        OPER_FIND_ALL = 3,
        OPER_DELETE = 4, 
        OPER_DELETE_ALL = 5 
}operationType_t;

// Datatypes supported by the MAGI C library for MongoDB Operations.
typedef enum{
        INT_TYPE = 1,
        CHAR_TYPE = 2,
        STRING_TYPE = 3,
        DOUBLE_TYPE = 4
}dataType_t;

// Structure of the node which holds the key-value pair for the document.
struct keyValueNode{
  char* key;
  dataType_t type;
  void* value;
  struct keyValueNode* next;
};

typedef struct keyValueNode* keyValueNode_t;

void mongoDBExecute(operationType_t operation, keyValueNode_t pair);

#endif /* _DATABASE_H */

