#!/usr/bin/env python

import hashlib
import logging
import pickle
import threading
import time

from magi.messaging.magimessage import MAGIMessage
from magi.util import database, helpers, config
from magi.util.agent import NonBlockingDispatchAgent, agentmethod
import pymongo
import yaml

from magi.db.Collection import AGENT_FIELD_KEY


log = logging.getLogger(__name__)

def getAgent():
        return DataManAgent()


class DataManAgent(NonBlockingDispatchAgent):
    
    def __init__(self, *args, **kwargs):
        try:
            NonBlockingDispatchAgent.__init__(self, *args, **kwargs)
            self.collectionMetadata = dict()
            self.events = dict()
            self.rcvdPongs = set()
            self.dbProcesses = set()
            self.dbLogHandler = None
            self.setupDatabase()
            #self.setupDBLogHandler()
        except:
            log.exception('Exception while initializing data manager')
            self.stop(None)
            
    def setupDatabase(self):
        functionName = self.setupDatabase.__name__
        helpers.entrylog(log, functionName, locals())
        
        log.info("Setting up database")
        if database.isDBSharded():
            log.info("Setting up a distributed database")
            if database.isConfigHost():
                log.info("Starting mongo config server")
                cp = database.startConfigServer()
                if cp: #if a database server was started with this call
                    self.dbProcesses.add(cp)
                log.info("Starting mongo shard server")
                sp = database.startShardServer()
                if sp: #if a database server was started with this call
                    self.dbProcesses.add(sp)
                log.info("Starting mongo database server")
                dp = database.startDBServer()
                if dp: #if a database server was started with this call
                    self.dbProcesses.add(dp)
                log.info("Stopping balancer")
                database.setBalancerState(False)
                log.info("Configuring database cluster")
                self.configureDBCluster()
            elif database.isCollector():
                log.info("Starting mongo database server")
                dp = database.startDBServer()
                if dp: #if a database server was started with this call
                    self.dbProcesses.add(dp)
                    
            log.info("Waiting for collector database to be added as a shard")
            database.isShardRegistered(dbHost=database.getCollector(), block=True)
            log.info("Collector database has been added as a shard")
            
        else:
            log.info("Setting up a non-distributed database")
            if database.isCollector():
                log.info("Starting mongo database server")
                dp = database.startDBServer()
                if dp: #if a database server was started with this call
                    self.dbProcesses.add(dp)
            
        log.info("Waiting for collector database to be up and running")        
        database.getConnection()
        log.info("Collector database up")
                
        helpers.exitlog(log, functionName)
         
    def configureDBCluster(self):
        """
            Function to configure the mongo db setup for an experiment.
            This is an internal function called by the bootstrap process.
        """
        functionName = self.configureDBCluster.__name__
        helpers.entrylog(log, functionName, locals())
        
        sensorToCollectorMap = database.getSensorToCollectorMap()
        
        log.info("Registering collector database servers as shards")
        cnodes = set(sensorToCollectorMap.values())
        for collector in cnodes:
            database.registerShard(collector)
            
        log.info("Configuring database cluster according to the sensor:collector mapping")
        snodes = set(sensorToCollectorMap.keys())
        if helpers.ALL in sensorToCollectorMap:
            allnodes = set(config.getTopoGraph().nodes())
            snodes.remove(helpers.ALL)
            rnodes = allnodes - snodes
        else:
            rnodes = set()
            
        for sensor in snodes:
            database.moveChunk(sensor, 
                               sensorToCollectorMap[sensor])
            database.moveChunk(sensor, 
                               sensorToCollectorMap[sensor], 
                               database.LOG_COLLECTION_NAME)
            
        for sensor in rnodes:
            database.moveChunk(sensor, 
                               sensorToCollectorMap[helpers.ALL])
            database.moveChunk(sensor, 
                               sensorToCollectorMap[helpers.ALL], 
                               database.LOG_COLLECTION_NAME)
        
        log.info("Creating index for %s.%s on key '%s'" %(database.DB_NAME, 
                                                          database.COLLECTION_NAME, 
                                                          AGENT_FIELD_KEY))
        configConn = database.getConnection(host=config.getDbConfigHost(), 
                                            port=database.ROUTER_SERVER_PORT)
        configConn[database.DB_NAME][database.COLLECTION_NAME].create_index(AGENT_FIELD_KEY)
        
        helpers.exitlog(log, functionName)
       
    def setupDBLogHandler(self):
        functionName = self.setupDBLogHandler.__name__
        helpers.entrylog(log, functionName, locals())
        
        log.info("Setting up database log handler")
        from magi.util.databaseLogHandler import DatabaseHandler
        self.dbLogHandler = DatabaseHandler.to(level=logging.INFO)
        rootLogger = logging.getLogger()
        rootLogger.addHandler(self.dbLogHandler)
        
        helpers.exitlog(log, functionName)
    
    @agentmethod()
    def stop(self, msg):
        if self.dbLogHandler:
            log.info("Removing database log handler")
            rootLogger = logging.getLogger()
            rootLogger.removeHandler(self.dbLogHandler)
            
        log.info("Terminating database processes")
        for p in self.dbProcesses:
            try:
                log.info("Terminating process %d", p.pid)
                p.terminate()
            except:
                log.exception("Could not terminate process %d", p.pid)
        
        log.info("Waiting for database processes to terminate")
        for p in self.dbProcesses:
            p.wait()
            log.info("Process %d terminated", p.pid)
        log.info("All database processes terminated")
        
        NonBlockingDispatchAgent.stop(self, msg)
            
    @agentmethod()
    def getData(self, msg, agents=None, nodes=None, filters=dict(), timestampChunks=None, visited=set()):
        """
            Request to fetch data
        """
        functionName = self.getData.__name__
        helpers.entrylog(log, functionName, locals())
        
        agents_ = helpers.toSet(agents)
        nodes_ = helpers.toSet(nodes)
        
        if not nodes_:
            nodes_ = config.getTopoGraph().nodes()
            
        if not agents_:
            if nodes:
                agents_ = self.getSensorAgents(nodes[0])
            else:
                raise AttributeError("Cannot query for an empty set of collections.")
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        data = dict()
        for agent in agents_:
            data[agent] = dict()
            for node in nodes_:
                filters_copy = filters.copy()
                filters_copy['host'] = node
                nodedata = []
                for tsChunk in timestampChunks:
                    nodedata = nodedata + database.getData(agent, 
                                                           filters_copy, 
                                                           tsChunk, 
                                                           database.configHost(), 
                                                           database.ROUTER_SERVER_PORT)
                data[agent][node] = nodedata
        
        args = {
            "agents": agents,
            "nodes": nodes,
            "filters": filters,
            "timestampChunks": timestampChunks,
            "visited": visited,
            "data": data
        }
        call = {'version': 1.0, 'method': 'putData', 'args': args}
        log.debug('Creating data message')
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.PICKLE, data=pickle.dumps(call))
        log.debug('Sending message')
        self.messenger.send(msg)
        
        helpers.exitlog(log, functionName)
        
    def getSensorAgents(self, node):
        """
            Internal function to fetch the list of sensor agents for a given node
        """
        call = {'version': 1.0, 'method': 'getCollectionMetadata'}
        querymsg = MAGIMessage(nodes=node, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))  
        queryHash = self.digest("CollectionMetadata", node)
        log.debug("getCollectionMetadata Query Hash: " + queryHash)
        self.events[queryHash] = threading.Event()
        self.messenger.send(querymsg)
        self.events[queryHash].wait()
        
        return self.collectionMetadata[node].value.keys()

    def isCollector(self, node, agentName):
        """
            Internal function to check if the local node is the collector for a given node and agent
        """
        return (self.getCollector(node, agentName) == config.getNodeName())
    
    def getCollector(self, node, agentName):
        """
            Internal function to fetch the collector for a given node and agent
        """
        functionName = self.getCollector.__name__
        helpers.entrylog(log, functionName, locals())
        
        node = node.split(".")[0]
        
        sensorToCollectorMap = database.getSensorToCollectorMap()
        result = sensorToCollectorMap.get(node, sensorToCollectorMap.get('__ALL__'))
        helpers.exitlog(log, functionName, result)
        return result
        
    @agentmethod()
    def getCollectionMetadata(self, msg):
        """
            Request for collector information
        """
        functionName = self.getCollectionMetadata.__name__
        helpers.entrylog(log, functionName, locals())
        
        args = {
            "collectionMetadata" : database.collectionHosts
        }
        call = {'version': 1.0, 'method': 'putCollectionMetadata', 'args': args}
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
        helpers.exitlog(log, functionName)
    
    @agentmethod()
    def putCollectionMetadata(self, msg, collectionMetadata):
        """
            Response for collector information request
        """
        functionName = self.putCollectionMetadata.__name__
        helpers.entrylog(log, functionName, locals())
        
        self.collectionMetadata[msg.src] = CachedItem(msg.src, collectionMetadata)
        queryHash = self.digest("CollectionMetadata", msg.src)
        self.events[queryHash].set()
        self.events[queryHash].clear()
        
        helpers.exitlog(log, functionName)
    
    @agentmethod()
    def ping(self, msg):
        """
            Alive like method call that will send a pong back to the caller
        """
        args = {
            "server": config.getNodeName(),
            "result": "success"
        }
        call = {'version': 1.0, 'method': 'pong', 'args': args}
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
    @agentmethod()
    def pong(self, msg, server, result):
        self.rcvdPongs.add(server)
        
    def checkIfUp(self, host, timeout=5):
        """
            Test call to check if data manager agent is available on a given node
        """
        call = {'version': 1.0, 'method': 'ping'}
        msg = MAGIMessage(nodes=host, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
        stop = time.time() + timeout
        while time.time() < stop:
            if host in self.rcvdPongs:
                return True
            
        return False
            
    def digest(self, *args):
        functionName = self.digest.__name__
        helpers.entrylog(log, functionName, locals())
        m = hashlib.md5()
        for arg in args:
            if type(arg) is set:
                arg = list(arg)
            if type(arg) is list:
                arg.sort()
            m.update(str(arg))
        result = m.hexdigest()
        helpers.exitlog(log, functionName, result)
        return result
    
class CachedItem(object):
    def __init__(self, key, value, duration=60):
        self.key = key
        self.value = value
        self.duration = duration
        self.timeStamp = time.time()
    
    def isValid(self):
        return self.timeStamp + self.duration > time.time()
    
    def __repr__(self):
        return '<CachedItem {%s:%s} expires at: %s>' % (self.key, self.value, self.timeStamp + self.duration)

