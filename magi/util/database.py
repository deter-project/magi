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
TYPE_FIELD = 'type'

# TODO: timeout should be dependent on machine type 
TIMEOUT = 900

dmconfig = config.loadConfig(config.DEFAULT_DBCONF);
configNode = dmconfig.get('config_node')
collectorMapping = dmconfig.get('collector_mapping')

nodeconfig = config.getConfig()
dbhost = nodeconfig.get('dbhost')
isDBHost = nodeconfig.get('is_dbhost')
isDBConfigServer = nodeconfig.get('is_db_config_server')

if 'connectionMap' not in locals():
    connectionMap = dict()
if 'collectionMap' not in locals():
    collectionMap = dict()
if 'collectionHosts' not in locals():
    collectionHosts = dict()
    collectionHosts['log'] = dbhost
    
def startConfigServer(timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongo config server is already running")
        if isDBRunning(port=27019):
            return

        try:
            os.makedirs('/data/configdb')  # Make sure mongodb config data directory is around
        except OSError, e:
            if e.errno != errno.EEXIST:
                log.error("failed to create mondodb config data dir: %s", e, exc_info=1)
                raise

        log.info("Trying to start mongo config server")
        mongod = ['/usr/local/bin/mongod', '--configsvr', '--dbpath', '/data/configdb', '--port', '27019']
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
    connection = getConnection(configNode, 27019)
    connection.config.settings.update({ "_id": "balancer" }, { "$set" : { "stopped": not state } } , True )
    
def startShardServer(configNode=configNode, timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongos server is already running")
        if isDBRunning(port=27017):
            return

        log.info("Trying to connect to mongo config server")
        getConnection(configNode, port=27019, timeout=timeout)
        
        log.info("Trying to start mongo shard server")
        mongos = ['/usr/local/bin/mongos', '--configdb', configNode + ":27019", '--port', '27017', '--noAutoSplit', '--logpath', '/tmp/mongos.log']
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
        if isDBRunning(port=27018):
            return

        if configfile is None:
            configfile = createMongoDConfig()
            
        mongo_conf = helpers.readPropertiesFile(configfile)

        try:
            os.makedirs(mongo_conf['dbpath'])  # Make sure mongodb data directory is around
        except OSError, e:
            if e.errno != errno.EEXIST:
                log.error("failed to create mondodb data dir: %s", e, exc_info=1)
                raise

        try:
            os.makedirs('/'.join(mongo_conf['logpath'].split('/')[:-1]))  # Make sure mongodb log directory is around
        except OSError, e:
            if e.errno != errno.EEXIST:
                log.error("failed to create mondodb log dir: %s", e, exc_info=1)
                raise

        log.info("Trying to start mongo database server")
        mongod = ['/usr/local/bin/mongod', '--config', configfile, '--shardsvr', '--journal', '--smallfiles']
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
        log.error("Exception while setting up mongo db database server: %s", e)
        raise

def createMongoDConfig():
    """
        Function to create a default Mongo DB configuration file
    """
    try:
        log.info("Creating mongo db config file.....")
        configfile = '/tmp/mongod.conf'
        f = open(configfile, 'w')
        f.write('dbpath=/var/lib/mongodb\n')
        f.write('logpath=/var/log/mongodb/mongodb.log\n')
        f.write('logappend=true\n')
        f.close() 
    except Exception, e:
        log.error("Failed to create mongodb default configuration file: %s", e)
        raise
    return configfile

def registerShard(mongod=testbed.nodename, mongos=testbed.getServer(), timeout=TIMEOUT):
    """
        Function to register a database server as a shard in the database cluster
    """
    functionName = registerShard.__name__
    entrylog(functionName, locals())
    
    start = time.time()
    stop = start + timeout
    log.info("Trying to register %s as a shard on %s" %(mongod, mongos))
    connection = getConnection(mongos, port=27017, timeout=timeout) #check if mongos is up and connect to it
    getConnection(mongod, port=27018, timeout=timeout) #check if mongod is up
    while time.time() < stop:
        if call("""/usr/local/bin/mongo --host %s --eval "sh.addShard('%s:27018')" """ %(mongos, mongod), shell=True):
            log.debug("Failed to add shard. Will retry.")
            time.sleep(1)
            continue
        if connection.config.shards.find({"host": "%s:27018" % mongod}).count() == 0:
            log.debug("Failed to add shard. Will retry.")
            time.sleep(1)
            continue
        log.info("Registered %s as a shard on %s" %(mongod, mongos))
        exitlog(functionName, locals())
        return
    
    log.error("Done trying enough times. Cannot add the required shard")
    exitlog(functionName, locals())
    raise pymongo.errors.PyMongoError("Done trying enough times. Cannot add the required shard")

def isShardRegistered(dbhost=testbed.nodename, configHost=configNode, block=False):
    """
        Check if given mongo db host is registered as a shard
    """
    functionName = isShardRegistered.__name__
    entrylog(functionName, locals())
    
    connection = getConnection(configHost, port=27017)
    log.info("Checking if database server is registered as a shard")
    while True:
        try:
            if connection.config.shards.find({"host": "%s:27018" % dbhost}).count() != 0:
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
    
    adminConnection = getConnection(testbed.getServer(), port=27017)
    
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
            adminConnection.admin.command('moveChunk', '%s.%s' %(DB_NAME, collectionname), find={"host": host}, to='%s:%d' %(collector, 27018))
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
        
    for sensor in rnodes:
        moveChunk(sensor, collectorMapping[helpers.ALL])
    
    log.info('Creating index on field: %s' %(TYPE_FIELD))
    getConnection(dbhost='localhost', port=27017)[DB_NAME][COLLECTION_NAME].ensure_index([(TYPE_FIELD, pymongo.ASCENDING)])
    
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
        
def getConnection(dbhost=None, port=27018, block=True, timeout=TIMEOUT):
    """
        Function to get connection to a database server
    """
    functionName = getConnection.__name__
    entrylog(functionName, locals())
    
    global connectionMap
    
    if dbhost == None:
        dbhost = getDBHost()
        
    if (dbhost, port) not in connectionMap:
        log.info("Trying to connect to mongodb server at %s:%d" %(dbhost, port))
        start = time.time()
        stop = start + timeout 
        while time.time() < stop:
            try:
                if dbhost == testbed.nodename: #In case of a single node experiment /etc/hosts does not get populated
                    connection = MongoClient('localhost', port)
                else:
                    connection = MongoClient(dbhost, port)
                connectionMap[(dbhost, port)] = connection
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
    return connectionMap[(dbhost, port)]
            
def getCollection(collection, dbhost=None):
    """
        Function to get a pointer to a given collection
    """
    functionName = getCollection.__name__
    entrylog(functionName, locals())
    
    global collectionMap
    global collectionHosts
    
    if dbhost == None:
        dbhost = getDBHost()
        
    if (collection, dbhost) not in collectionMap:
        try:
            if collectionHosts[collection] != dbhost:
                log.error("Multiple db hosts for same collection")
                raise Exception("Multiple db hosts for same collection")
        except KeyError:
            collectionHosts[collection] = dbhost
        collectionMap[(collection, dbhost)] = Collection(collection, dbhost)
    
    exitlog(functionName, locals())
    return collectionMap[(collection, dbhost)]

def getData(collection, filters=None, timestampRange=None, connection=None):
    """
        Function to retrieve data from the local database, based on a given query
    """
    functionName = getData.__name__
    entrylog(functionName, locals())
        
    if not isDBHost:
        return None

    if connection == None:
        connection = getConnection('localhost')
            
    if filters == None:
        filters_copy = dict()
    else:
        filters_copy = filters.copy()
        
    if timestampRange:
        ts_start, ts_end = timestampRange
        filters_copy['created'] = {'$gte': ts_start, '$lte': ts_end}
    
    filters_copy[TYPE_FIELD] = collection
    cursor = connection[DB_NAME][COLLECTION_NAME].find(filters_copy)
    
    result = []
    
    while True:
        try:
            result.append(cursor.next())
        except StopIteration:
            break
    
    exitlog(functionName)
    return result

def getDBHost():
    return dbhost

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

    def __init__(self, collectiontype, dbhost=None):
        if dbhost == None:
            dbhost = getDBHost()
        connection = getConnection(dbhost, port=27018)
        self.collection = connection[DB_NAME][COLLECTION_NAME]
        self.type = collectiontype

    def insert(self, **kwargs):
        """
            Function to insert data. Adds the default fields before insertion.
        """
        kwargs['host'] = testbed.nodename
        kwargs['created'] = time.time()
        kwargs[TYPE_FIELD] = self.type
        self.collection.insert(kwargs)
        
    def remove(self, **kwargs):
        """
            Function to remove data from the connection database server.
            Only data corresponding to the class instance's host and type will be removed.
        """
        kwargs['host'] = testbed.nodename
        kwargs[TYPE_FIELD] = self.type
        self.collection.remove(kwargs)
        
#    def removeAll(self):
#        kwargs = dict()
#        kwargs[TYPE_FIELD] = self.type
#        self.collection.remove(kwargs)


## Functions to activate caching of data

#def updateDatabase(collectionname, filters=None, timestampChunks=None, data=[], connection=None):
#    """
#        Function to update the local database
#    """
#    functionName = updateDatabase.__name__
#    entrylog(functionName, locals())
#    
#    if not isDBHost:
#        return
#        
#    if connection == None:
#        connection = getConnection('localhost')
#                
#    collection = connection[DB_NAME][collectionname]
#        
#    for record in data:
#        try:
#            collection.insert(record)
#        except StopIteration:
#            break
#        except:
#            continue
#        
#    updateMetadata(collectionname, filters, timestampChunks, connection)
#    
#    exitlog(functionName)

#def updateMetadata(collectionname, filters=None, timestampChunks=None, connection=None):
#    """
#        Function to update metadata about the local database
#    """
#    functionName = updateMetadata.__name__
#    entrylog(functionName, locals())
#    
#    if not timestampChunks:
#        return
#    
#    if not isDBHost:
#        return
#        
#    if filters == None:
#        filters = dict()
#        
#    if connection == None:
#        connection = getConnection('localhost')
#                
#    itr = connection['cache']['metadata'].find({'db': DB_NAME, 'collection': collectionname})        
#    while True:
#        try:
#            record = itr.next()
#            rec_filters = ast.literal_eval(record['filters'])
#            
#            #if filters == {k: rec_filters[k] for k in filters.keys() if k in rec_filters}:
#            sub_rec_filters = dict()
#            for k in filters.keys():
#                if k in rec_filters:
#                    sub_rec_filters[k] = rec_filters[k]
#                    
#            if filters == sub_rec_filters:
#                ts_chunks = record['ts_chunks']
#                ts_chunks = insertChunks(ts_chunks, timestampChunks)
#                connection['cache']['metadata'].update({'_id': record['_id'] }, { '$set': { 'ts_chunks': ts_chunks } })                        
#
#        except StopIteration:
#            break
#
#    itr = connection['cache']['metadata'].find({'db': DB_NAME, 'collection': collectionname, 'filters': str(filters)})
#    try:
#        record = itr.next()
#    except StopIteration:
#        record = {"db": DB_NAME, "collection": collectionname, 'filters': str(filters), 'ts_chunks': timestampChunks}
#        log.debug("cache.metadata insertion: " + str(record))
#        connection['cache']['metadata'].insert(record)
#                
#    exitlog(functionName)

#def findTimeRangeNotAvailable(collectionname, filters=None, timestampRange=None, connection=None):
#    functionName = findTimeRangeNotAvailable.__name__
#    entrylog(functionName, locals())
#    
#    if timestampRange == None:
#        timestampRange = (0, time.time())
#        
#    if not isDBHost:
#        return [timestampRange]
#
#    availableTimeRange = getAvailableTimeRange(collectionname, filters, connection)
#    missingTimeRange = findMissingTimeRange(availableTimeRange, timestampRange)
#
#    exitlog(functionName, missingTimeRange)
#    return missingTimeRange
#
#
#def getAvailableTimeRange(collectionname, filters=None, connection=None):
#    functionName = getAvailableTimeRange.__name__
#    entrylog(functionName, locals())
#    
#    if not isDBHost:
#        return []
#            
#    if filters == None:
#        filters = dict()
#        
#    result = []
#            
#    filterKeys = filters.keys()
#    
#    for subsetLength in range(len(filterKeys)+1):
#        filterKeysSubsets = itertools.combinations(filterKeys, subsetLength)
#        for filterKeysSubset in filterKeysSubsets:
#            #subsetFilters = {k: filters[k] for k in filterKeysSubset}
#            subsetFilters = dict()
#            for k in filterKeysSubset:
#                subsetFilters[k] = filters[k]
#            availableTimeRange = getAvailableTimeRangeForExactFilter(collectionname, subsetFilters, connection)
#            result = insertChunks(result, availableTimeRange)
#            
#    exitlog(functionName)
#    return result
#        
#def getAvailableTimeRangeForExactFilter(collectionname, filters=None, connection=None):
#    functionName = getAvailableTimeRangeForExactFilter.__name__
#    entrylog(functionName, locals())
#    
#    if not isDBHost:
#        return []
#    
#    if filters == None:
#        filters = dict()
#    
#    if connection == None:
#        connection = getConnection('localhost')
#    
#    itr = connection['cache']['metadata'].find({'db': DB_NAME, 'collection': collectionname})
#    
#    while True:
#        try:
#            record = itr.next()
#            log.debug(record)
#            rec_filters = ast.literal_eval(record['filters'])
#            if filters == rec_filters:
#                result = record['ts_chunks']
#                break
#        except StopIteration:
#                result = []
#                break
#            
#    exitlog(functionName)
#    return result
#
#def findMissingTimeRange(availableTimeRange, requiredTimeRange):
#    functionName = findMissingTimeRange.__name__
#    entrylog(functionName)
#            
#    if availableTimeRange == None:
#        return [requiredTimeRange]
#        
#    chunksNotAvailable = []
#    reqdStart, reqdEnd = requiredTimeRange
#
#    availableTimeRange.sort(reverse=True)
#
#    for chunkStart, chunkEnd in availableTimeRange:
#                
#        if reqdStart > chunkEnd:
#            chunksNotAvailable = chunksNotAvailable + [(reqdStart, reqdEnd)]
#            exitlog(functionName, chunksNotAvailable)
#            return chunksNotAvailable
#                            
#        if reqdEnd > chunkEnd:
#            chunksNotAvailable = chunksNotAvailable + [(chunkEnd, reqdEnd)]
#                                
#        if reqdStart >= chunkStart:
#            exitlog(functionName, chunksNotAvailable)
#            return chunksNotAvailable
#                                    
#        if reqdEnd > chunkStart:
#            reqdEnd = chunkStart
#
#    chunksNotAvailable = chunksNotAvailable + [(reqdStart, reqdEnd)]
#
#    exitlog(functionName)
#    return chunksNotAvailable
#
#def insertChunks(existingChunks, newChunks):
#        functionName = insertChunks.__name__
#        entrylog(functionName)
#    
#        if not newChunks:
#            return existingChunks
#        
#        if not existingChunks:
#            return newChunks
#        
#        existingChunks.sort(reverse=True)
#        newChunks.sort(reverse=True)
#        
#        result = existingChunks[:]
#        ptr = 0
#        
#        for newChunk in newChunks:
#            
#            newStart, newEnd = newChunk
#            
#            while ptr < len(result):
#                chunk = result[ptr]
#                chunkStart, chunkEnd = chunk
#                
#                if newStart > chunkEnd:
#                    break
#
#                elif newEnd < chunkStart:
#                    ptr += 1
#                
#                else:
#                    if newEnd < chunkEnd:
#                        newEnd = chunkEnd
#                    if newStart > chunkStart:
#                        newStart = chunkStart
#                    result.remove(chunk)
#                
#            result.insert(ptr, (newStart, newEnd))
#            
#        exitlog(functionName)
#        return result

    

##Old functions that have been replaced

#def insertChunk(existingChunks=[], newChunk=None):
#        
#        if newChunk == None:
#                return existingChunks
#        
#        existingChunks.sort()
#        
#        result = existingChunks[:]
#                
#        newStart, newEnd = newChunk
#        
#        for chunk in existingChunks:
#                chunkStart, chunkEnd = chunk
#                
#                if newEnd < chunkStart:
#                        result.insert(result.index(chunk), (newStart, newEnd))
#                        return result
#                
#                if newStart <= chunkEnd:
#                        if newStart > chunkStart:
#                                newStart = chunkStart
#                        if newEnd < chunkEnd:
#                                newEnd = chunkEnd
#                        result.remove(chunk)
#        
#        result.append((newStart, newEnd))
#        return result
     
#def findMissingChunks(availableChunks, requiredChunk):
#        
#        if availableChunks == None:
#                return [requiredChunk]
#        
#        chunksNotAvailable = []
#        reqdStart, reqdEnd = requiredChunk
#
#        availableChunks.sort()
#        
#        for chunkStart, chunkEnd in availableChunks:
#                
#                if reqdEnd < chunkStart:
#                        chunksNotAvailable = chunksNotAvailable + [(reqdStart, reqdEnd)]
#                        return chunksNotAvailable
#                            
#                if reqdStart < chunkStart:
#                        chunksNotAvailable = chunksNotAvailable + [(reqdStart, chunkStart)]
#                                
#                if reqdEnd <= chunkEnd:
#                        return chunksNotAvailable
#                                    
#                if reqdStart < chunkEnd:
#                        reqdStart = chunkEnd
#
#        chunksNotAvailable = chunksNotAvailable + [(reqdStart, reqdEnd)]
#
#        return chunksNotAvailable