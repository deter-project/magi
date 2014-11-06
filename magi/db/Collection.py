import logging 
import time

from magi.util import helpers
import pymongo

from Connection import getConnection
from magi.db import DATABASE_SERVER_PORT


log = logging.getLogger(__name__)

DB_NAME = 'magi'
COLLECTION_NAME = 'experiment_data'
AGENT_FIELD = 'agent'

if 'collectionCache' not in locals():
    collectionCache = dict()
    
def getCollection(agentName, hostName, dbHost='localhost', dbPort=DATABASE_SERVER_PORT):
    """
        Function to get a pointer to a given agent data collection
    """
    functionName = getCollection.__name__
    helpers.entrylog(log, functionName, locals())
    
    global collectionCache
    
    if (agentName, dbHost, dbPort) not in collectionCache:
        collectionCache[(agentName, hostName, dbHost, dbPort)] = Collection(agentName, hostName, dbHost, dbPort)
    
    helpers.exitlog(log, functionName)
    return collectionCache[(agentName, hostName, dbHost, dbPort)]

class Collection(pymongo.collection.Collection):
    """Library to use for data collection"""
    
    INTERNAL_KEYS = ['host', 'created', AGENT_FIELD]

    def __init__(self, agentName, hostName, dbHost='localhost', dbPort=DATABASE_SERVER_PORT):
        connection = getConnection(host=dbHost, port=dbPort, block=False)
        pymongo.collection.Collection.__init__(self, connection[DB_NAME], COLLECTION_NAME)
        self.agentName = agentName
        self.hostName = hostName

    def insert(self, doc_or_docs, *args, **kwargs):
        """
            Insert data. Add the default fields before insertion.
        """
        if isinstance(doc_or_docs, dict):
            docs = [doc_or_docs]
            
        for doc in docs:
            if not isinstance(doc, dict):
                raise TypeError("each document must be an instance of dict")
            if len(set(Collection.INTERNAL_KEYS) & set(doc.keys())) > 0:
                raise RuntimeError("The following keys are restricted for internal use: %s" %(Collection.INTERNAL_KEYS))
            doc['host'] = self.hostName
            doc['created'] = time.time()
            doc[AGENT_FIELD] = self.agentName
            
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
        
        spec['host'] = self.hostName
        spec[AGENT_FIELD] = self.agentName
        
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
        
        spec[AGENT_FIELD] = self.agentName
        
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
            
        spec['host'] = self.hostName
        spec[AGENT_FIELD] = self.agentName
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
            
        spec[AGENT_FIELD] = self.agentName
        return pymongo.collection.Collection.remove(self, spec_or_id, safe, **kwargs)

        
#    def removeAll(self):
#        kwargs = dict()
#        kwargs[AGENT_FIELD] = self.type
#        self.collection.remove(kwargs)
