#!/usr/bin/env python

import logging
import os
from subprocess import Popen, call
import sys
import tempfile
import time

from magi.db import CONFIG_SERVER_PORT, ROUTER_SERVER_PORT, DATABASE_SERVER_PORT
from magi.util import helpers
import pymongo

from Collection import HOST_FIELD_KEY
from Connection import getConnection


log = logging.getLogger(__name__)

TIMEOUT=900
TEMP_DIR = tempfile.gettempdir()

def startConfigServer(port=CONFIG_SERVER_PORT, 
                      dbPath=os.path.join(TEMP_DIR, "configdb"), 
                      logPath=os.path.join(TEMP_DIR, "mongoc.log"),
                      block=True,
                      timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    if timeout <= 0:
        timeout = sys.maxint
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongo config server is already running")
        if isDBRunning(port=port):
            return
        
        try:
            helpers.makeDir(dbPath)  # Make sure mongodb config data directory is around
        except OSError, e:
            log.exception("failed to create mondodb config data dir")
            raise

        log.info("Trying to start mongo config server")
        mongod = ['mongod', '--configsvr', 
                  '--dbpath', dbPath, 
                  '--port', str(port), 
                  '--logpath', logPath]
        log.info("Running %s", mongod)
        
        while time.time() < stop:
            p = Popen(mongod)
            time.sleep(1)
            if p.poll() is None:
                log.info("Started mongod config server with pid %s", p.pid)
                return p
            log.debug("Failed to start mongod config server. Will retry.")
            if not block:
                break
            log.debug("Retrying to start mongo config server")
            
        log.error("Cannot start mongod config server")
        raise pymongo.errors.PyMongoError("Cannot start mongod config server")
    
    except Exception, e:
        log.error("Exception while setting up mongo db config server: %s", e)
        raise

def setBalancerState(state, configHost, configPort=CONFIG_SERVER_PORT):
    """
        Function to turn on/off data balancer
    """
    connection = getConnection(configHost, configPort)
    connection.config.settings.update({ "_id": "balancer" }, { "$set" : { "stopped": not state } } , True )
    
def startShardServer(port=ROUTER_SERVER_PORT, 
                     logPath= os.path.join(TEMP_DIR, "mongos.log"),
                     configHost=None, 
                     configPort=CONFIG_SERVER_PORT, 
                     block=True,
                     timeout=TIMEOUT):
    """
        Function to start a database config server on the node
    """
    if timeout <= 0:
        timeout = sys.maxint
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongos server is already running")
        if isDBRunning(port=port):
            return

        log.info("Trying to connect to mongo config server")
        getConnection(configHost, port=configPort, block=block, timeout=timeout)
        
        log.info("Trying to start mongo shard server")
        mongos = ['mongos', '--configdb', '%s:%d'%(configHost, configPort), 
                  '--port', str(port), 
                  '--noAutoSplit', 
                  '--logpath', logPath]
        log.info("Running %s", mongos)
        
        while time.time() < stop:
            p = Popen(mongos)
            time.sleep(1)
            if p.poll() is None:
                log.info("Started mongo shard server with pid %s", p.pid)
                return p
            log.debug("Failed to start shard config server.")
            if not block:
                break
            log.debug("Retrying to start mongo shard server")
            
        log.error("Cannot start mongo shard server")
        raise pymongo.errors.PyMongoError("Cannot start mongo shard server")
    
    except Exception, e:
        log.error("Exception while setting up mongo db shard server: %s", e)
        raise

def startDBServer(port=DATABASE_SERVER_PORT,
                  configfile=None,
                  configDir=TEMP_DIR, 
                  dbPath=os.path.join(TEMP_DIR, "mongodb"), 
                  logPath=os.path.join(TEMP_DIR, "mongodb.log"),
                  block=True,
                  timeout=TIMEOUT):
    """
        Function to start a database server on the node
    """
    if timeout <= 0:
        timeout = sys.maxint
    start = time.time()
    stop = start + timeout
    
    try:
        log.info("Checking if an instance of mongod server is already running")
        if isDBRunning(port=port):
            return

        if configfile is None:
            configfile = createMongoDConfig(configDir=configDir,
                                            dbPath=dbPath, 
                                            logPath=logPath)
            
        mongo_conf = helpers.readPropertiesFile(configfile)

        try:
            helpers.makeDir(mongo_conf['dbpath'])  # Make sure mongodb data directory is around
        except:
            log.exception("failed to create mondodb data dir: %s", mongo_conf['dbpath'])
            raise

        try:
            logdir = os.path.dirname(mongo_conf['logpath'])
            helpers.makeDir(logdir)  # Make sure mongodb log directory is around
        except:
            log.exception("failed to create mondodb log dir: %s", logdir)
            raise

        log.info("Trying to start mongo database server")
        mongod = ['mongod', 
                  '--config', configfile, 
                  '--port', str(port), 
                  '--shardsvr', 
                  '--journal', 
                  '--smallfiles']
        log.info("Running %s", mongod)
        
        while time.time() < stop:
            p = Popen(mongod)
            time.sleep(1)
            if p.poll() is None:
                log.info("Started mongod with pid %s", p.pid)
                return p
            log.debug("Failed to start mongod server.")
            if not block:
                break
            log.debug("Retrying to start mongo database server")
            
        log.error("Cannot start database server")
        raise pymongo.errors.PyMongoError("Cannot start database server")
    
    except:
        log.exception("Exception while setting up mongo db database server")
        raise

def createMongoDConfig(configDir=TEMP_DIR,
                       dbPath=os.path.join(TEMP_DIR, "mongodb"), 
                       logPath=os.path.join(TEMP_DIR, "mongodb.log")):
    """
        Function to create a default Mongo DB configuration file
    """
    try:
        log.info("Creating mongo db config file")
        configfile = os.path.join(configDir, "mongod.conf")
        f = open(configfile, 'w')
        f.write('dbpath=%s\n'%(dbPath))
        f.write('logpath=%s\n'%(logPath))
        f.write('logappend=true\n')
        f.close() 
    except:
        log.exception("Failed to create mongo db default configuration file")
        raise
    return configfile

def registerShard(dbHost, configHost, dbPort=DATABASE_SERVER_PORT, configPort=ROUTER_SERVER_PORT, block=True, timeout=TIMEOUT):
    """
        Function to register a database server as a shard in the database cluster
    """
    functionName = registerShard.__name__
    helpers.entrylog(log, functionName, locals())
    
    if not block:
        timeout = 0
    elif timeout <= 0:
        timeout = sys.maxint
        
    start = time.time()
    stop = start + timeout
    
    log.info("Trying to register %s:%d as a shard on %s:%d" %(dbHost, dbPort, configHost, configPort))
    connection = getConnection(host=configHost, port=configPort, timeout=timeout) #check if mongos is up and connect to it
    getConnection(host=dbHost, port=DATABASE_SERVER_PORT, timeout=timeout) #check if mongod is up
    
    while time.time() < stop:
        if call("""/usr/local/bin/mongo --host %s --eval "sh.addShard('%s:%d')" """ %(configHost, dbHost, dbPort), shell=True):
            log.debug("Failed to add shard. Will retry.")
            time.sleep(1)
            continue
        if connection.config.shards.find({HOST_FIELD_KEY: "%s:%d" % (dbHost, dbPort)}).count() == 0:
            log.debug("Failed to add shard. Will retry.")
            time.sleep(1)
            continue
        log.info("Registered %s as a shard on %s" %(dbHost, configHost))
        helpers.exitlog(log, functionName)
        return
    
    log.error("Cannot add the required shard")
    helpers.exitlog(log, functionName)
    raise pymongo.errors.PyMongoError("Cannot add the required shard")

def isShardRegistered(dbHost, configHost, dbPort=DATABASE_SERVER_PORT, configPort=ROUTER_SERVER_PORT, block=True):
    """
        Check if given mongo db host is registered as a shard
    """
    functionName = isShardRegistered.__name__
    helpers.entrylog(log, functionName, locals())
    
    connection = getConnection(host=configHost, port=configPort)
    log.info("Checking if database server is registered as a shard")
    while True:
        try:
            if connection.config.shards.find({HOST_FIELD_KEY: "%s:%d" %(dbHost, dbPort)}).count() != 0:
                helpers.exitlog(log, functionName)
                return True
        except:
            pass
        if not block:
            helpers.exitlog(log, functionName)
            return False
        time.sleep(1)
    
def moveChunk(db, collection, host, collector, configHost, configPort=ROUTER_SERVER_PORT):
    """
        Shard, split and move a given collection to the corresponding collector
    """
    functionName = moveChunk.__name__
    helpers.entrylog(log, functionName, locals())
    
    adminConnection = getConnection(host=configHost, port=configPort)
    
    log.info("Trying to move chunk %s:%s to %s" %(host, collection, collector))
    
    while True:
        try:
            log.info("Enabling sharding %s.%s" %(db, collection))
            adminConnection.admin.command('enablesharding', '%s.%s' %(db, collection))
            log.info("Sharding enabled successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #sharding might already be enabled
            if "already enabled" in str(e):
                break
            time.sleep(0.2)
        
    log.info("Creating index for %s.%s on key '%s'" %(db, collection, HOST_FIELD_KEY))
    adminConnection[db][collection].create_index(HOST_FIELD_KEY)
    log.info("Index created successfully")
    
    while True:
        try:
            log.info("Sharding Collection %s.%s" %(db, collection))
            adminConnection.admin.command('shardcollection', '%s.%s' %(db, collection), key={HOST_FIELD_KEY: 1})
            log.info("Collection sharded successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #might already be sharded
            if "already sharded" in str(e):
                break
            time.sleep(0.2)
    
    while True:
        try:
            log.info("Splitting Collection %s.%s on host:%s" %(db, collection, host))
            adminConnection.admin.command("split", '%s.%s' %(db, collection), middle={HOST_FIELD_KEY: host})
            log.info("Collection split successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #might already be sharded
            if "cannot split on initial or final" in str(e):
                break
            time.sleep(0.2)
            
    while True:
        try:
            log.info("Moving chunk %s.%s {'host': %s} to %s" %(db, collection, host, collector))
            adminConnection.admin.command('moveChunk', '%s.%s' %(db, collection), 
                                          find={HOST_FIELD_KEY: host}, 
                                          to='%s:%d' %(collector, DATABASE_SERVER_PORT))
            log.info("Collection moved successfully.")
            break
        except pymongo.errors.OperationFailure, e:
            log.error(str(e)) #might already be sharded
            if "that chunk is already on that shard" in str(e):
                break
            time.sleep(0.2)
            
    helpers.exitlog(log, functionName)

def isDBRunning(host='localhost', port=None):
    """
        Check if a database server is running on a given host and port
    """
    try:        
        getConnection(host=host, port=port, block=False)
        log.info("An instance of mongodb server is already running on %s:%d" %(host, port))
        return True
    except pymongo.errors.ConnectionFailure:
        log.info("No instance of mongodb server is already running on %s:%d" %(host, port))
        return False

