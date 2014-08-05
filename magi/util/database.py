#!/usr/bin/env python

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage
from magi.testbed import testbed
from magi.util import config, helpers

from pymongo import MongoClient
from pymongo.database import Database
from subprocess import Popen, call

import Queue
import ast
import errno
import itertools
import logging
import os
import pickle
import pymongo
import random
import time
import yaml

log = logging.getLogger(__name__)

DB_NAME = 'magi'
COLLECTION_NAME = 'experiment_data'
AGENT_FIELD = 'agent'
LOG_COLLECTION_NAME = 'log'

DATABASE_SERVER_PORT = 27018
ROUTER_SERVER_PORT   = 27017
CONFIG_SERVER_PORT   = 27019

# TODO: timeout should be dependent on machine type 
TIMEOUT = 900

dbConfig = config.getConfig().get('database', {})

isDBEnabled         = dbConfig.get('isDBEnabled', False)
configHost          = dbConfig.get('configHost')
collectorMapping    = dbConfig.get('collectorMapping')

collector = collectorMapping.get(testbed.nodename, collectorMapping.get('__ALL__'))
isConfigHost = (testbed.nodename == configHost)
isCollector = (testbed.nodename in collectorMapping.values())
isSensor = (testbed.nodename in collectorMapping.keys() or '__ALL__' in collectorMapping.keys())


if 'connectionCache' not in locals():
    connectionCache = dict()
if 'collectionCache' not in locals():
    collectionCache = dict()
if 'collectionHosts' not in locals():
    collectionHosts = dict()
    collectionHosts['log'] = collector
    
def startConfigServer(timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongo config server is already running")
        if isDBRunning(port=CONFIG_SERVER_PORT):
            return

        try:
            os.makedirs('/data/configdb')  # Make sure mongodb config data directory is around
        except OSError, e:
            if e.errno != errno.EEXIST:
                log.exception("failed to create mondodb config data dir")
                raise

        log.info("Trying to start mongo config server")
        mongod = ['/usr/local/bin/mongod', '--configsvr', 
                  '--dbpath', '/data/configdb', 
                  '--port', str(CONFIG_SERVER_PORT), 
                  '--logpath', os.path.join(config.getLogDir(), "mongoc.log")]
        log.info("Running %s", mongod)
        
        while time.time() < stop:
            p = Popen(mongod)
            time.sleep(1)
            if p.poll() is None:
                log.info("Started mongod config server with pid %s", p.pid)
                return p.pid
            log.debug("Failed to start mongod config server. Will retry.")
            
        log.error("Done trying enough times. Cannot start mongod config server")
        raise pymongo.errors.PyMongoError("Done trying enough times. Cannot start mongod config server")
    
    except Exception, e:
        log.error("Exception while setting up mongo db config server: %s", e)
        raise

def setBalancerState(state):
    """
        Function to turn on/off data balancer
    """
    connection = getConnection(configHost, CONFIG_SERVER_PORT)
    connection.config.settings.update({ "_id": "balancer" }, { "$set" : { "stopped": not state } } , True )
    
def startShardServer(configHost=configHost, timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    configHost = helpers.toControlPlaneNodeName(configHost)
    
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongos server is already running")
        if isDBRunning(port=ROUTER_SERVER_PORT):
            return

        log.info("Trying to connect to mongo config server")
        getConnection(configHost, port=CONFIG_SERVER_PORT, timeout=timeout)
        
        log.info("Trying to start mongo shard server")
        mongos = ['/usr/local/bin/mongos', '--configdb', '%s:%d'%(configHost, CONFIG_SERVER_PORT), 
                  '--port', str(ROUTER_SERVER_PORT), 
                  '--noAutoSplit', 
                  '--logpath', os.path.join(config.getLogDir(), "mongos.log")]
        log.info("Running %s", mongos)
        
        while time.time() < stop:
            p = Popen(mongos)
            time.sleep(1)
            if p.poll() is None:
                log.info("Started mongo shard server with pid %s", p.pid)
                return p.pid
            log.debug("Failed to start shard config server. Will retry.")
            
        log.error("Done trying enough times. Cannot start mongo shard server")
        raise pymongo.errors.PyMongoError("Done trying enough times. Cannot start mongo shard server")
    
    except Exception, e:
        log.error("Exception while setting up mongo db shard server: %s", e)
        raise

def startDBServer(configfile=None, timeout=TIMEOUT):
    """
        Function to start a database server on the node
    """
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongod server is already running")
        if isDBRunning(port=DATABASE_SERVER_PORT):
            return

        if configfile is None:
            configfile = createMongoDConfig()
            
        mongo_conf = helpers.readPropertiesFile(configfile)

        try:
            os.makedirs(mongo_conf['dbpath'])  # Make sure mongodb data directory is around
        except OSError, e:
            if e.errno != errno.EEXIST:
                log.exception("failed to create mondodb data dir: %s", mongo_conf['dbpath'])
                raise

        try:
            logdir = '/'.join(mongo_conf['logpath'].split('/')[:-1])
            os.makedirs(logdir)  # Make sure mongodb log directory is around
        except OSError, e:
            if e.errno != errno.EEXIST:
                log.exception("failed to create mondodb log dir: %s", logdir)
                raise

        log.info("Trying to start mongo database server")
        mongod = ['/usr/local/bin/mongod', '--config', configfile, '--port', str(DATABASE_SERVER_PORT), '--shardsvr', '--journal', '--smallfiles']
        log.info("Running %s", mongod)
        
        while time.time() < stop:
            p = Popen(mongod)
            time.sleep(1)
            if p.poll() is None:
                log.info("Started mongod with pid %s", p.pid)
                return p.pid
            log.debug("Failed to start mongod server. Will retry.")
            
        log.error("Done trying enough times. Cannot start database server")
        raise pymongo.errors.PyMongoError("Done trying enough times. Cannot start database server")
    
    except Exception, e:
        log.exception("Exception while setting up mongo db database server")
        raise

def createMongoDConfig():
    """
        Function to create a default Mongo DB configuration file
    """
    try:
        log.info("Creating mongo db config file")
        configfile = '/tmp/mongod.conf'
        f = open(configfile, 'w')
        f.write('dbpath=%s\n'%(config.getDbDir()))
        f.write('logpath=%s/mongodb.log\n'%(config.getLogDir()))
        f.write('logappend=true\n')
        f.close() 
    except Exception, e:
        log.exception("Failed to create mongodb default configuration file")
        raise
    return configfile

def registerShard(mongod=testbed.nodename, mongos=testbed.getServer(), timeout=TIMEOUT):
    """
        Function to register a database server as a shard in the database cluster
    """
    functionName = registerShard.__name__
    entrylog(functionName, locals())
    
    mongod = helpers.toControlPlaneNodeName(mongod)
    mongos = helpers.toControlPlaneNodeName(mongos)
        
    start = time.time()
    stop = start + timeout
    log.info("Trying to register %s as a shard on %s" %(mongod, mongos))
    connection = getConnection(mongos, port=ROUTER_SERVER_PORT, timeout=timeout) #check if mongos is up and connect to it
    getConnection(mongod, port=DATABASE_SERVER_PORT, timeout=timeout) #check if mongod is up
    while time.time() < stop:
        if call("""/usr/local/bin/mongo --host %s --eval "sh.addShard('%s:%d')" """ %(mongos, mongod, DATABASE_SERVER_PORT), shell=True):
            log.debug("Failed to add shard. Will retry.")
            time.sleep(1)
            continue
        if connection.config.shards.find({"host": "%s:%d" % (mongod, DATABASE_SERVER_PORT)}).count() == 0:
            log.debug("Failed to add shard. Will retry.")
            time.sleep(1)
            continue
        log.info("Registered %s as a shard on %s" %(mongod, mongos))
        exitlog(functionName, locals())
        return
    
    log.error("Done trying enough times. Cannot add the required shard")
    exitlog(functionName, locals())
    raise pymongo.errors.PyMongoError("Done trying enough times. Cannot add the required shard")

def isShardRegistered(dbhost=testbed.nodename, configHost=configHost, block=False):
    """
        Check if given mongo db host is registered as a shard
    """
    functionName = isShardRegistered.__name__
    entrylog(functionName, locals())
    
    dbhost = helpers.toControlPlaneNodeName(dbhost)
    configHost = helpers.toControlPlaneNodeName(configHost)
        
    connection = getConnection(configHost, port=ROUTER_SERVER_PORT)
    log.info("Checking if database server is registered as a shard")
    while True:
        try:
            if connection.config.shards.find({"host": "%s:%d" %(dbhost, DATABASE_SERVER_PORT)}).count() != 0:
                exitlog(functionName, locals())
                return True
        except:
            pass
        if not block:
            exitlog(functionName, locals())
            return False
        time.sleep(1)
    
def moveChunk(host, collector=None, collectionname=COLLECTION_NAME):
    """
        Shard, split and move a given collection to the corresponding collector
    """
    functionName = moveChunk.__name__
    entrylog(functionName, locals())
    
    if collector == None:
        collector = host
    
    collector = helpers.toControlPlaneNodeName(collector)
        
    adminConnection = getConnection(testbed.getServer(), port=ROUTER_SERVER_PORT)
    
    log.info("Trying to move chunk %s:%s to %s" %(host, collectionname, collector))
    
    while True:
        try:
            log.info("Enabling sharding %s.%s" %(DB_NAME, collectionname))
            adminConnection.admin.command('enablesharding', '%s.%s' %(DB_NAME, collectionname))
            log.info("Sharding enabled successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #sharding might already be enabled
            if "already enabled" in str(e):
                break
            time.sleep(0.2)
        
    while True:
        try:
            log.info("Sharding Collection %s.%s" %(DB_NAME, collectionname))
            adminConnection.admin.command('shardcollection', '%s.%s' %(DB_NAME, collectionname), key={"host": 1})
            log.info("Collection sharded successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #might already be sharded
            if "already sharded" in str(e):
                break
            time.sleep(0.2)
    
    while True:
        try:
            log.info("Splitting Collection %s.%s on host:%s" %(DB_NAME, collectionname, host))
            adminConnection.admin.command("split", '%s.%s' %(DB_NAME, collectionname), middle={"host": host})
            log.info("Collection split successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #might already be sharded
            if "cannot split on initial or final" in str(e):
                break
            time.sleep(0.2)
            
    while True:
        try:
            log.info("Moving chunk %s.%s {'host': %s} to %s" %(DB_NAME, collectionname, host, collector))
            adminConnection.admin.command('moveChunk', '%s.%s' %(DB_NAME, collectionname), find={"host": host}, to='%s:%d' %(collector, DATABASE_SERVER_PORT))
            log.info("Collection moved successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #might already be sharded
            if "that chunk is already on that shard" in str(e):
                break
            time.sleep(0.2)
            
    exitlog(functionName, locals())

def configureDBCluster():
    """
        Function to configure the mongo db setup for an experiment.
        This is an internal function called by the bootstrap process.
    """
    functionName = configureDBCluster.__name__
    entrylog(functionName, locals())
    
    log.info("Registering collector database servers as shards")
    cnodes = set(collectorMapping.values())
    for collector in cnodes:
        registerShard(collector)
        
    log.info("Configuring database cluster acccording to the sensor:collector mapping")
    snodes = set(collectorMapping.keys())
    if helpers.ALL in collectorMapping:
        allnodes = set(testbed.getTopoGraph().nodes())
        snodes.remove(helpers.ALL)
        rnodes = allnodes - snodes
    else:
        rnodes = set()
        
    for sensor in snodes:
        moveChunk(sensor, collectorMapping[sensor])
        moveChunk(sensor, collectorMapping[sensor], 'log')
        
    for sensor in rnodes:
        moveChunk(sensor, collectorMapping[helpers.ALL])
        moveChunk(sensor, collectorMapping[helpers.ALL], 'log')
    
    log.info('Creating index on field: %s' %(AGENT_FIELD))
    getConnection(dbhost='localhost', port=ROUTER_SERVER_PORT)[DB_NAME][COLLECTION_NAME].ensure_index([(AGENT_FIELD, pymongo.ASCENDING)])
    
    exitlog(functionName, locals())
        
def checkIfAllCollectorsRegistered():
    """
        Check if all the collector database servers are registered as shards
    """
    cnodes = set(collectorMapping.values())
    for collector in cnodes:
        while True:
            log.info("Waiting for %s to be added as a shard" %(collector))
            if isShardRegistered(collector):
                break
            time.sleep(1)
        
def getConnection(dbhost=None, port=DATABASE_SERVER_PORT, block=True, timeout=TIMEOUT):
    """
        Function to get connection to a database server
    """
    functionName = getConnection.__name__
    entrylog(functionName, locals())
    
    global connectionCache
    
    if dbhost == None:
        dbhost = getCollector()
        
    dbhost = helpers.toControlPlaneNodeName(dbhost)
    
    if (dbhost, port) not in connectionCache:
        log.info("Trying to connect to mongodb server at %s:%d" %(dbhost, port))
        start = time.time()
        stop = start + timeout 
        while time.time() < stop:
            try:
                if dbhost == testbed.nodename: #In case of a single node experiment /etc/hosts does not get populated
                    connection = MongoClient('localhost', port)
                else:
                    connection = MongoClient(dbhost, port)
                connectionCache[(dbhost, port)] = connection
                log.info("Connected to mongodb server at %s:%d" %(dbhost, port))
                exitlog(functionName, locals())
                return connection
            except Exception:
                if not block:
                    log.error("Could not connect to mongodb server on %s:%d" %(dbhost, port))
                    raise
                log.debug("Could not connect to mongodb server. Will retry.")
                time.sleep(1)
                
        log.error("Done trying enough times. Cannot connect to mongodb server on %s", dbhost)
        raise pymongo.errors.ConnectionFailure("Done trying enough times. Cannot connect to mongodb server on %s" %dbhost)
    
    exitlog(functionName, locals())
    return connectionCache[(dbhost, port)]
            
def getCollection(agentName, dbhost=None):
    """
        Function to get a pointer to a given agent data collection
    """
    functionName = getCollection.__name__
    entrylog(functionName, locals())
    
    global collectionCache
    global collectionHosts
    
    if dbhost == None:
        dbhost = getCollector()
        
    if (agentName, dbhost) not in collectionCache:
        try:
            if collectionHosts[agentName] != dbhost:
                log.error("Multiple collectors for same agent")
                raise Exception("Multiple colelctors for same agent")
        except KeyError:
            collectionHosts[agentName] = dbhost
        collectionCache[(agentName, dbhost)] = Collection(agentName, dbhost)
    
    exitlog(functionName, locals())
    return collectionCache[(agentName, dbhost)]

def getData(agentName, filters=None, timestampRange=None, connection=None):
    """
        Function to retrieve data from the local database, based on a given query
    """
    functionName = getData.__name__
    entrylog(functionName, locals())
        
    if not isCollector:
        return None

    if connection == None:
        connection = getConnection()
            
    if filters == None:
        filters_copy = dict()
    else:
        filters_copy = filters.copy()
        
    if timestampRange:
        ts_start, ts_end = timestampRange
        filters_copy['created'] = {'$gte': ts_start, '$lte': ts_end}
    
    filters_copy[AGENT_FIELD] = agentName
    cursor = connection[DB_NAME][COLLECTION_NAME].find(filters_copy)
    
    result = []
    
    while True:
        try:
            result.append(cursor.next())
        except StopIteration:
            break
    
    exitlog(functionName)
    return result

def getCollector():
    return collector

def isDBRunning(host='localhost', port=None):
    """
        Check if a database server is running on a given host and port
    """
    try:        
        getConnection(dbhost=host, port=port, block=False)
        log.info("An instance of mongodb server is already running on %s:%d" %(host, port))
        return True
    except pymongo.errors.ConnectionFailure:
        log.info("No instance of mongodb server is already running on %s:%d" %(host, port))
        return False

def entrylog(functionName, arguments=None):
    if arguments == None:
        log.debug("Entering function %s", functionName)
    else:
        log.debug("Entering function %s with arguments: %s", functionName, arguments)

def exitlog(functionName, returnValue=None):
    if returnValue == None:
        log.debug("Exiting function %s", functionName)
    else:
        log.debug("Exiting function %s with return value: %s", functionName, returnValue)

class Collection():
    """Library to use for data collection"""
    
    INTERNAL_KEYS = ['host', 'created', AGENT_FIELD]

    def __init__(self, agentName, dbhost=None):
        if dbhost == None:
            dbhost = getCollector()
        connection = getConnection(dbhost, port=DATABASE_SERVER_PORT)
        self.collection = connection[DB_NAME][COLLECTION_NAME]
        self.agentName = agentName

    def insert(self, **kwargs):
        """
            Function to insert data. Adds the default fields before insertion.
        """
        if len(set(Collection.INTERNAL_KEYS) & set(kwargs.keys())) > 0:
            raise RuntimeError("The following keys are restricted for internal use: %s" %(Collection.INTERNAL_KEYS))
        kwargs['host'] = testbed.nodename
        kwargs['created'] = time.time()
        kwargs[AGENT_FIELD] = self.agentName
        return self.collection.insert(kwargs)
        
    def remove(self, **kwargs):
        """
            Function to remove data from the connection database server.
            Only data corresponding to the class instance's host and agentName will be removed.
        """
        kwargs['host'] = testbed.nodename
        kwargs[AGENT_FIELD] = self.agentName
        return self.collection.remove(kwargs)

    def find(self, **kwargs):
        """
        """
        kwargs['host'] = testbed.nodename
        kwargs[AGENT_FIELD] = self.agentName
        return self.collection.find(kwargs)
        
#    def removeAll(self):
#        kwargs = dict()
#        kwargs[AGENT_FIELD] = self.type
#        self.collection.remove(kwargs)
