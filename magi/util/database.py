#!/usr/bin/env python

from collections import defaultdict
import logging
import os

from magi.util import config, helpers

from magi.db import Server, Connection, Collection
from magi.db.Collection import DB_NAME, COLLECTION_NAME
from magi.db.Server import DATABASE_SERVER_PORT, ROUTER_SERVER_PORT, CONFIG_SERVER_PORT


log = logging.getLogger(__name__)

LOG_COLLECTION_NAME = 'logs'

# TODO: timeout should be dependent on machine type 
TIMEOUT = 900

dbConfig = config.getConfig().get('database', {})

isDBEnabled         = dbConfig.get('isDBEnabled', False)
isDBSharded         = dbConfig.get('isDBSharded', True)
configHost          = dbConfig.get('configHost')
sensorToCollectorMap    = dbConfig.get('sensorToCollectorMap', {})

collector = sensorToCollectorMap.get(config.getNodeName(), sensorToCollectorMap.get('__ALL__'))
isConfigHost = (config.getNodeName() == configHost)
isCollector = (config.getNodeName() in sensorToCollectorMap.values())
isSensor = (config.getNodeName() in sensorToCollectorMap.keys() or '__ALL__' in sensorToCollectorMap.keys())

if 'collectionHosts' not in locals():
    collectionHosts = defaultdict(set)
    
def startConfigServer(timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    return Server.startConfigServer(port=CONFIG_SERVER_PORT, 
                             dbPath=os.path.join(config.getDbDir(), "configdb"), 
                             logPath=os.path.join(config.getLogDir(), "mongoc.log"), 
                             timeout=timeout)

def setBalancerState(state):
    """
        Function to turn on/off data balancer
    """
    Server.setBalancerState(state=state, 
                            configHost=configHost, 
                            configPort=CONFIG_SERVER_PORT)
    
def startShardServer(configHost=configHost, timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    configHost = helpers.toControlPlaneNodeName(configHost)
    
    return Server.startShardServer(port=ROUTER_SERVER_PORT, 
                            logPath=os.path.join(config.getLogDir(), "mongos.log"), 
                            configHost=configHost, 
                            configPort=CONFIG_SERVER_PORT, 
                            timeout=timeout)

def startDBServer(configfile=None, timeout=TIMEOUT):
    """
        Function to start a database server on the node
    """
    return Server.startDBServer(port=DATABASE_SERVER_PORT, 
                         configfile=configfile, 
                         dbPath=os.path.join(config.getDbDir(), "mongodb"), 
                         logPath=os.path.join(config.getLogDir(), "mongodb.log"), 
                         timeout=timeout)

def registerShard(mongod=config.getNodeName(), mongos=config.getServer(), timeout=TIMEOUT):
    """
        Function to register a database server as a shard in the database cluster
    """
    functionName = registerShard.__name__
    helpers.entrylog(log, functionName, locals())
    
    mongod = helpers.toControlPlaneNodeName(mongod)
    mongos = helpers.toControlPlaneNodeName(mongos)
        
    Server.registerShard(dbHost=mongod, configHost=mongos, timeout=timeout)

    helpers.exitlog(log, functionName)

def isShardRegistered(dbHost=config.getNodeName(), configHost=configHost, block=False):
    """
        Check if given mongo db host is registered as a shard
    """
    functionName = isShardRegistered.__name__
    helpers.entrylog(log, functionName, locals())
    
    dbHost = helpers.toControlPlaneNodeName(dbHost)
    configHost = helpers.toControlPlaneNodeName(configHost)
        
    helpers.exitlog(log, functionName)
    return Server.isShardRegistered(dbHost=dbHost, 
                                    configHost=configHost, 
                                    dbPort=DATABASE_SERVER_PORT, 
                                    configPort=CONFIG_SERVER_PORT, 
                                    block=block)
    
def moveChunk(host, collector=None, collectionname=COLLECTION_NAME):
    """
        Shard, split and move a given collection to the corresponding collector
    """
    functionName = moveChunk.__name__
    helpers.entrylog(log, functionName, locals())
    
    if collector == None:
        collector = host
    
    collector = helpers.toControlPlaneNodeName(collector)
        
    Server.moveChunk(db=DB_NAME, collection=collectionname, 
                     host=host, 
                     collector=collector, 
                     configHost=config.getServer(), 
                     configPort=ROUTER_SERVER_PORT)
            
    helpers.exitlog(log, functionName)
        
def getConnection(host='localhost', port=DATABASE_SERVER_PORT, block=True, timeout=TIMEOUT):
    """
        Function to get connection to a database server
    """
    functionName = getConnection.__name__
    helpers.entrylog(log, functionName, locals())
    
    if host == None:
        host = getCollector()
        
    if host == config.getNodeName(): #In case of a single node experiment /etc/hosts does not get populated
        host = 'localhost'
    else:
        host = helpers.toControlPlaneNodeName(host)
    
    helpers.exitlog(log, functionName)
    return Connection.getConnection(host, port, block, timeout)
            
def getCollection(agentName, dbHost=config.getNodeName(), dbPort=DATABASE_SERVER_PORT):
    """
        Function to get a pointer to a given agent data collection
    """
    functionName = getCollection.__name__
    helpers.entrylog(log, functionName, locals())
    
    if dbHost == None:
        dbHost = getCollector()
    
    global collectionHosts
    collectionHosts[agentName].add(dbHost)
    
    helpers.exitlog(log, functionName)
    return Collection.getCollection(agentName, config.getNodeName(), dbHost, dbPort)

def isDBRunning(host='localhost', port=DATABASE_SERVER_PORT):
    """
        Check if a database server is running on a given host and port
    """
    return Server.isDBRunning(host=host, port=port)

def getData(agentName, filters=None, timestampRange=None, dbHost='localhost', dbPort=DATABASE_SERVER_PORT):
    """
        Function to retrieve data from the local database, based on a given query
    """
    functionName = getData.__name__
    helpers.entrylog(log, functionName, locals())
        
    if filters == None:
        filters_copy = dict()
    else:
        filters_copy = filters.copy()
        
    if timestampRange:
        ts_start, ts_end = timestampRange
        filters_copy['created'] = {'$gte': ts_start, '$lte': ts_end}
    
    collection = Collection.getCollection(agentName, config.getNodeName(), dbHost, dbPort)
    cursor = collection.findAll(filters_copy)
    
    result = []
    
    while True:
        try:
            result.append(cursor.next())
        except StopIteration:
            break
    
    helpers.exitlog(log, functionName)
    return result

def getCollector():
    return collector
