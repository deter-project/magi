import logging
import sys
import time

from magi.util import helpers
from pymongo import MongoClient
import pymongo

from magi.db import DATABASE_SERVER_PORT

log = logging.getLogger(__name__)
TIMEOUT=900

if 'connectionCache' not in locals():
    connectionCache = dict()
    
def getConnection(host='localhost', port=DATABASE_SERVER_PORT, block=True, timeout=TIMEOUT):
    """
        Function to get connection to a database server
    """
    functionName = getConnection.__name__
    helpers.entrylog(log, functionName, locals())
    
    global connectionCache
    
    if (host, port) not in connectionCache:
        
        log.debug("Required connection not available in cache")
        
        if timeout <= 0:
            timeout = sys.maxint
        start = time.time()
        stop = start + timeout 
        
        while time.time() < stop:
            try:
                log.debug("Trying to connect to mongodb server at %s:%d" %(host, port))
                connection = Connection(host, port)
                connectionCache[(host, port)] = connection
                log.info("Connected to mongodb server at %s:%d" %(host, port))
                helpers.exitlog(log, functionName)
                return connection
            except:
                time.sleep(1)
            if not block:
                break
                
        log.error("Could not connect to mongodb server on %s:%d" %(host, port))
        raise pymongo.errors.ConnectionFailure("Could not connect to mongodb server on %s:%d" %(host, port))
    
    helpers.exitlog(log, functionName)
    return connectionCache[(host, port)]

class Connection(MongoClient):
    """
        Connection to MongoDB.
    """
    def __init__(self, host=None, port=None):
        super(Connection, self).__init__(host, port)
