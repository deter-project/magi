import logging 
import time

from Connection import Connection
from magi.db import DATABASE_SERVER_PORT
from magi.util import helpers
import pymongo

from Connection import getConnection


log = logging.getLogger(__name__)

DB_NAME = 'magi'
COLLECTION_NAME = 'experiment_data'
HOST_FIELD_KEY = 'host'
CREATED_TS_FIELD_KEY = 'created'
AGENT_FIELD_KEY = 'agent'


if 'collectionCache' not in locals():
    collectionCache = dict()
    
def getCollection(agentName, hostName, connection=None, dbHost='localhost', dbPort=DATABASE_SERVER_PORT):
    """
        Function to get a pointer to a given agent data collection
    """
    functionName = getCollection.__name__
    helpers.entrylog(log, functionName, locals())
    
    global collectionCache

    if connection:
        if not isinstance(connection, Connection):
            raise TypeError("Invalid connection instance")    
        dbHost = connection.host
        dbPort = connection.port
        
    if (agentName, dbHost, dbPort) not in collectionCache:
        collectionCache[(agentName, hostName, dbHost, dbPort)] = Collection(agentName=agentName, 
                                                                            hostName=hostName, 
                                                                            connection=connection,
                                                                            dbHost=dbHost, 
                                                                            dbPort=dbPort)
    
    helpers.exitlog(log, functionName)
    return collectionCache[(agentName, hostName, dbHost, dbPort)]

class Collection(pymongo.collection.Collection):
    """Library to use for data collection"""
    
    INTERNAL_KEYS = [HOST_FIELD_KEY, CREATED_TS_FIELD_KEY, AGENT_FIELD_KEY]

    def __init__(self, agentName, hostName, connection=None, dbHost='localhost', dbPort=DATABASE_SERVER_PORT):
        if not connection:
            connection = getConnection(host=dbHost, port=dbPort, block=False)
        pymongo.collection.Collection.__init__(self, connection[DB_NAME], COLLECTION_NAME)
        self.agentName = agentName
        self.hostName = hostName

    def insert(self, doc_or_docs, *args, **kwargs):
        """
            Insert data. Add the default fields before insertion.
        """
        if (not isinstance(doc_or_docs, dict)) and (not isinstance(doc_or_docs, list)):
            raise TypeError("document(s) must be an instance of dict or a list of dicts")
            
        if isinstance(doc_or_docs, dict):
            docs = [doc_or_docs]
            
        for doc in docs:
            if not isinstance(doc, dict):
                raise TypeError("each document must be an instance of dict")
            if len(set(Collection.INTERNAL_KEYS) & set(doc.keys())) > 0:
                raise RuntimeError("The following keys are restricted for internal use: %s" %(Collection.INTERNAL_KEYS))
            doc[HOST_FIELD_KEY] = self.hostName
            doc[CREATED_TS_FIELD_KEY] = time.time()
            doc[AGENT_FIELD_KEY] = self.agentName
            
        return pymongo.collection.Collection.insert(self, docs, *args, **kwargs)
        
    def find(self, *args, **kwargs):
        """
            Find data corresponding to the class instance's agent and host.
        """
        if not args:
            args = [{}]
        spec = args[0]
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        
        spec[HOST_FIELD_KEY] = self.hostName
        spec[AGENT_FIELD_KEY] = self.agentName
        
        return pymongo.collection.Collection.find(self, *args, **kwargs)
    
    def findAll(self, *args, **kwargs):
        """
            Find data corresponding to the class instance's agent, irrespective of the host.
        """
        if not args:
            args = [{}]
        spec = args[0]
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        
        spec[AGENT_FIELD_KEY] = self.agentName
        
        return pymongo.collection.Collection.find(self, *args, **kwargs)
    
    def remove(self, spec_or_id=None, safe=None, **kwargs):
        """
            Remove data corresponding to the class instance's agent and host.
        """
        if spec_or_id is None:
            spec_or_id = {}
            
        if not isinstance(spec_or_id, dict):
            spec = {"_id": spec_or_id}
        else:
            spec = spec_or_id
            
        spec[HOST_FIELD_KEY] = self.hostName
        spec[AGENT_FIELD_KEY] = self.agentName
        return pymongo.collection.Collection.remove(self, spec_or_id, safe, **kwargs)

    def removeAll(self, spec_or_id=None, safe=None, **kwargs):
        """
            Remove data corresponding to the class instance's agent, irrespective of the host.
        """
        if spec_or_id is None:
            spec_or_id = {}
            
        if not isinstance(spec_or_id, dict):
            spec = {"_id": spec_or_id}
        else:
            spec = spec_or_id
            
        spec[AGENT_FIELD_KEY] = self.agentName
        return pymongo.collection.Collection.remove(self, spec_or_id, safe, **kwargs)

        
#    def removeAll(self):
#        kwargs = dict()
#        kwargs[AGENT_FIELD_KEY] = self.type
#        self.collection.remove(kwargs)
