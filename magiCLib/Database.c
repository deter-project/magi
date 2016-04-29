/**
 * This file is the MAGI C Database library for interaction with MongoDB.
 * Insert, Find, Delete and Display functions are implemented here.
 */
#include "Database.h"
#include "AgentMessenger.h"
#include "AgentTransport.h"

#include <stdio.h>
#include <sys/time.h>

extern char *agentName, *hostName, *expDBLocation, *expDBPort;

/**
 * Inserts a key-value pair to the mongoDB.
 * 
 * @param: pair - the keyValueNode_t head node of the key-value linked list. 
 * @param: collection - the mongoc_collection_t * collection item where document needs to be added.
 */
void MongoDBInsertExecute(keyValueNode_t pair, mongoc_collection_t *collection)
{
    mongoc_bulk_operation_t *bulk;
    bson_error_t error;
    bson_t *doc;
    bson_oid_t oid;
    bson_t reply;
    char *str;
    bool ret;
    int i = 0;
    double *dblVal = (double*)malloc(sizeof(double));

    bulk = mongoc_collection_create_bulk_operation (collection, true, NULL);

    /**
     * Loop through all the nodes in the linked list and add it in the bulk operation.
     * Insert the bulk data in the mongodb.
     * Data is appeneded to the bulk local variable as per the data type.
     */
    keyValueNode_t temp = pair;
    doc = bson_new ();
    bson_oid_init (&oid, NULL);
    BSON_APPEND_OID (doc, "_id", &oid);
    while (temp != NULL){
        switch(temp->type){
         case INT_TYPE:
            BSON_APPEND_INT32 (doc, temp->key, temp->value);
            break;
         case DOUBLE_TYPE:
            dblVal = (double*)temp->value;
            BSON_APPEND_DOUBLE (doc, temp->key, *dblVal);
            break;
         case STRING_TYPE:
            BSON_APPEND_UTF8 (doc, temp->key, (char*)temp->value);
            break;
        }
        temp = temp->next;
    }

    BSON_APPEND_UTF8 (doc, "agent", agentName);
    BSON_APPEND_UTF8 (doc, "host", hostName);

    struct timeval now;
    gettimeofday(&now, NULL);
    double *time = (double*) malloc(sizeof(double));
    *time = now.tv_sec + (double)now.tv_usec/1000000;

    BSON_APPEND_DOUBLE (doc, "created", *time);
    mongoc_bulk_operation_insert (bulk, doc);
    bson_destroy (doc);
    ret = mongoc_bulk_operation_execute (bulk, &reply, &error);

    str = bson_as_json (&reply, NULL);
    printf ("%s\n", str);
    bson_free (str);

    if (!ret) {
        fprintf (stderr, "Error: %s\n", error.message);
    }

    bson_destroy (&reply);
    mongoc_bulk_operation_destroy (bulk);
}


/**
 * Finds a specific key-value pair document in the mongoDB.
 * 
 * @param: pair - the keyValueNode_t node which needs to be looked up in mongoDB.
 * @param: collection - the mongoc_collection_t * collection item from where data is retrieved.
 */
void MongoDBFindExecute(keyValueNode_t pair, mongoc_collection_t *collection)
{
   const bson_t *doc;
   bson_t *query;
   char *str;
   mongoc_cursor_t *cursor;

   keyValueNode_t temp = pair;
   query = bson_new ();

   while (temp != NULL){
       switch(temp->type){
         case INT_TYPE:
            BSON_APPEND_INT32 (query, temp->key, temp->value);
            break;
         case STRING_TYPE:
            BSON_APPEND_UTF8 (query, temp->key, temp->value);
            break;
       }
       temp = temp->next;
   }
   BSON_APPEND_UTF8 (query, "agent", agentName);
   BSON_APPEND_UTF8 (query, "host", hostName);

   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

   while (mongoc_cursor_next (cursor, &doc)) {
       str = bson_as_json (doc, NULL);
       printf ("%s\n", str);
       bson_free (str);
   }
   bson_destroy (query);
}


/**
 * Displays all the documents in a mongoDB collection.
 * 
 * @param: collection - the mongoc_collection_t * collection item from where data is retrieved.
 */
void MongoDBFindAllExecute(mongoc_collection_t *collection)
{
   const bson_t *doc;
   bson_t *query;
   char *str;
   mongoc_cursor_t *cursor;

   query = bson_new ();
   BSON_APPEND_UTF8 (query, "agent", agentName);
   BSON_APPEND_UTF8 (query, "host", hostName);

   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

 
   while (mongoc_cursor_next (cursor, &doc)) {
       str = bson_as_json (doc, NULL);
       printf ("%s\n", str);
       bson_free (str);
   }
}


/**
 * Deletes the document from the mongoDB collection.
 * 
 * @param: pair - the keyValueNode_t head node of the key-value linked list to be deleted. 
 * @param: collection - the mongoc_collection_t * collection item where document needs to be deleted.
 */
void MongoDBDeleteExecute(keyValueNode_t pair, mongoc_collection_t *collection)
{
    bson_error_t error;
    bson_t *doc;
    int i;

    keyValueNode_t temp = pair;

    while (temp != NULL){
        doc = bson_new ();
        switch(temp->type){
         case INT_TYPE:
            BSON_APPEND_INT32 (doc, temp->key, temp->value);
            break;
         case STRING_TYPE:
            BSON_APPEND_UTF8 (doc, temp->key, temp->value);
            break;
        }
        temp = temp->next;
    }

    BSON_APPEND_UTF8 (doc, "agent", agentName);
    BSON_APPEND_UTF8 (doc, "host", hostName);

    if (!mongoc_collection_remove(collection, MONGOC_REMOVE_SINGLE_REMOVE, doc, NULL, &error)) {
        printf ("Delete failed: %s\n", error.message);
    }
    bson_destroy (doc);
}


/**
 * Deletes all the documents from the mongoDB collection.
 * 
 * @param: collection - the mongoc_collection_t * collection item where document needs to be deleted.
 */
void MongoDBDeleteAllExecute(mongoc_collection_t *collection)
{
    bson_error_t error;
    bson_t *doc;
    int i;

    doc = bson_new ();
    BSON_APPEND_UTF8 (doc, "agent", agentName);
    BSON_APPEND_UTF8 (doc, "host", hostName);

    if (!mongoc_collection_remove(collection, MONGOC_REMOVE_NONE, doc, NULL, &error)) {
            printf ("Delete failed: %s\n", error.message);
    }
    bson_destroy (doc);
}

/**
 * This method makes function calls as per the operationType_t enum (INSERT, FIND, DISPLAY, DELETE).
 * It is invoked by the user from the agent code.
 * 
 * @param: operation - the operation which the user wants to perform.
 * @param: pair - the keyValueNode_t head node of the key-value linked list passed by user. 
 */
void mongoDBExecute(operationType_t operation, keyValueNode_t pair)
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_init ();
   char mongoClient[100];
   sprintf(mongoClient, "mongodb://%s:%s/", expDBLocation, expDBPort);
   client = mongoc_client_new (mongoClient);
   collection = mongoc_client_get_collection (client, "magi", "experiment_data");
 
   switch(operation){
      case OPER_INSERT:
          MongoDBInsertExecute(pair, collection);
          break;
      case OPER_FIND:
          MongoDBFindExecute(pair, collection);
          break;
      case OPER_FIND_ALL:
          MongoDBFindAllExecute(collection);
          break;
      case OPER_DELETE:
          MongoDBDeleteExecute(pair, collection);
          break;
      case OPER_DELETE_ALL:
          MongoDBDeleteAllExecute(collection);
          break;
   }

   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);

   mongoc_cleanup ();
}


/**
 * Util function used by user to append the key-value pair in the agent code.
 * 
 * @param: head - the keyValueNode_t head node of the key-value linked list.
 * @param: pair - the keyValueNode_t node of the key-value to be added in the linked list. 
 */
void mongoDBAppendList(keyValueNode_t head, keyValueNode_t node)
{
   if (head == NULL){
      head = node;
   } else{
      node->next = head;
      head = node;
   }
}
