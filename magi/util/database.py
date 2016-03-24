#!/usr/bin/env python

from collections import defaultdict
import logging
import os

from magi.db import DATABASE_SERVER_PORT, ROUTER_SERVER_PORT, CONFIG_SERVER_PORT
from magi.db import Server, Connection, Collection
from magi.db.Collection import DB_NAME, COLLECTION_NAME
from magi.util import config, helpers


log = logging.getLogger(__name__)

LOG_COLLECTION_NAME = 'logs'

# TODO: timeout should be dependent on machine type 
TIMEOUT = 900

if 'collectionHosts' not in locals():
    collectionHosts = defaultdict(set)
    
def getDbConfig():
    return config.getConfig().get('database', {})

def isDBEnabled():
    return getDbConfig().get('isDBEnabled', False)

def setDBStatus(status):
    if type(status) != bool:
        return
    getDbConfig()['isDBEnabled'] = False
    
def isDBSharded():
    return getDbConfig().get('isDBSharded', True)
    
def getConfigHost():
    return getDbConfig().get('globalServerHost')
    
def getSensorToCollectorMap():
    return getDbConfig().get('sensorToCollectorMap', {})
    
def getCollector():
    sensorToCollectorMap = getSensorToCollectorMap()
    return sensorToCollectorMap.get(config.getNodeName(), 
                                    sensorToCollectorMap.get(helpers.DEFAULT))
    
def isConfigHost():
    configHost = getConfigHost()
    return (config.getNodeName() == configHost or 
            helpers.toControlPlaneNodeName(config.getNodeName()) == configHost)
    
def isCollector():
    sensorToCollectorMap = getSensorToCollectorMap()
    return (config.getNodeName() in sensorToCollectorMap.values())

def isSensor():
    sensorToCollectorMap = getSensorToCollectorMap()
    return (config.getNodeName() in sensorToCollectorMap.keys() 
            or helpers.DEFAULT in sensorToCollectorMap.keys())
    
def startConfigServer(timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    return Server.startConfigServer(
                    dbPath=os.path.join(config.getDbDir(), "configdb"), 
                    logPath=os.path.join(config.getLogDir(), "mongoc.log"), 
                    timeout=timeout)

def setBalancerState(state):
    """
        Function to turn on/off data balancer
    """
    Server.setBalancerState(state=state, 
                    configHost=helpers.toControlPlaneNodeName(getConfigHost()))
    
def startShardServer(configHost=getConfigHost(), timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    return Server.startShardServer(
                        logPath=os.path.join(config.getLogDir(), "mongos.log"), 
                        configHost=helpers.toControlPlaneNodeName(configHost), 
                        timeout=timeout)

def startDBServer(configfile=None, timeout=TIMEOUT):
    """
        Function to start a database server on the node
    """
    helpers.makeDir(config.getDbDir())
    return Server.startDBServer( 
                        configfile=configfile,
                        configDir=config.getConfigDir(), 
                        dbPath=os.path.join(config.getDbDir(), "mongodb"), 
                        logPath=os.path.join(config.getLogDir(), "mongodb.log"), 
                        timeout=timeout)

def registerShard(mongod=config.getNodeName(), mongos=getConfigHost(), 
                  timeout=TIMEOUT):
    """
        Function to register a database server as a shard 
        in the database cluster
    """
    functionName = registerShard.__name__
    helpers.entrylog(log, functionName, locals())
    
    mongod = helpers.toControlPlaneNodeName(mongod)
    mongos = helpers.toControlPlaneNodeName(mongos)
        
    Server.registerShard(dbHost=mongod, configHost=mongos, timeout=timeout)

    helpers.exitlog(log, functionName)

def isShardRegistered(dbHost=None, configHost=getConfigHost(), block=False):
    """
        Check if given mongo db host is registered as a shard
    """
    functionName = isShardRegistered.__name__
    helpers.entrylog(log, functionName, locals())
    
    if dbHost == None:
        dbHost = getCollector()
        
    helpers.exitlog(log, functionName)
    return Server.isShardRegistered(
                        dbHost=helpers.toControlPlaneNodeName(dbHost), 
                        configHost=helpers.toControlPlaneNodeName(configHost), 
                        block=block)
    
def moveChunk(host, collector=None, collectionname=COLLECTION_NAME):
    """
        Shard, split and move a given collection to the corresponding collector
    """
    functionName = moveChunk.__name__
    helpers.entrylog(log, functionName, locals())
    
    if collector == None:
        collector = host
    
    Server.moveChunk(db=DB_NAME, collection=collectionname, 
                     host=host, 
                     collector=helpers.toControlPlaneNodeName(collector), 
                     configHost=helpers.toControlPlaneNodeName(getConfigHost()))
            
    helpers.exitlog(log, functionName)
        
def getConnection(host=None, port=DATABASE_SERVER_PORT, block=True, 
                  timeout=TIMEOUT):
    """
        Function to get connection to a database server
    """
    functionName = getConnection.__name__
    helpers.entrylog(log, functionName, locals())
    
    if host == None:
        host = getCollector()
        
    #In case of a single node experiment /etc/hosts does not get populated
    if host == config.getNodeName(): 
        host = 'localhost'
    
    helpers.exitlog(log, functionName)
    return Connection.getConnection(host=helpers.toControlPlaneNodeName(host), 
                                    port=port, block=block, timeout=timeout)
            
def getCollection(agentName, dbHost=None, dbPort=DATABASE_SERVER_PORT):
    """
        Function to get a pointer to a given agent data collection
    """
    functionName = getCollection.__name__
    helpers.entrylog(log, functionName, locals())
    
    if dbHost == None:
        dbHost = getCollector()
    
    global collectionHosts
    collectionHosts[agentName].add(dbHost)
    
    connection = getConnection(host=dbHost, port=dbPort, block=False)
    
    helpers.exitlog(log, functionName)
    return Collection.getCollection(agentName=agentName, 
                                    hostName=config.getNodeName(),
                                    connection=connection)

def isDBRunning(host='localhost', port=DATABASE_SERVER_PORT):
    """
        Check if a database server is running on a given host and port
    """
    #In case of a single node experiment /etc/hosts does not get populated
    if host == config.getNodeName(): 
        host = 'localhost'
    return Server.isDBRunning(host=helpers.toControlPlaneNodeName(host), 
                              port=port)

def getData(agentName, filters=None, timestampRange=None,
             dbHost='localhost', dbPort=DATABASE_SERVER_PORT):
    """
        Function to retrieve data from the local database, 
        based on a given query
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
    
    collection = getCollection(agentName=agentName, 
                               dbHost=dbHost, 
                               dbPort=dbPort)
    cursor = collection.findAll(filters_copy)
    
    result = []
    
    while True:
        try:
            result.append(cursor.next())
        except StopIteration:
            break
    
    helpers.exitlog(log, functionName)
    return result

