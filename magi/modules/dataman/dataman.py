#!/usr/bin/env python

from magi.messaging.magimessage import MAGIMessage
from magi.testbed import testbed
from magi.util import database, helpers
from magi.util.agent import NonBlockingDispatchAgent, agentmethod

import hashlib
import logging
import pickle
import threading
import time
import yaml

log = logging.getLogger(__name__)

def getAgent():
        return DataManAgent()


class DataManAgent(NonBlockingDispatchAgent):
    
    def __init__(self, *args, **kwargs):
        NonBlockingDispatchAgent.__init__(self, *args, **kwargs)
        self.collectionMetadata = dict()
        self.events = dict()
        self.rcvdPongs = set()
        
    @agentmethod()
    def getData(self, msg, agents=None, nodes=None, filters=dict(), timestampChunks=None, visited=set()):
        """
            Request to fetch data
        """
        functionName = self.getData.__name__
        entrylog(functionName, locals())
        
        agents_ = helpers.toSet(agents)
        nodes_ = helpers.toSet(nodes)
        
        if not nodes_:
            nodes_ = testbed.getTopoGraph().nodes()
            
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
                    nodedata = nodedata + database.getData(agent, filters_copy, tsChunk, database.getConnection(database.configHost, port=database.ROUTER_SERVER_PORT))
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
        
        exitlog(functionName)
        
    def getSensorAgents(self, node):
        """
            Internal function to fetch the list of collections for a given node
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
        return (self.getCollector(node, agentName) == testbed.getNodeName())
    
    def getCollector(self, node, agentName):
        """
            Internal function to fetch the collector for a given node and agent
        """
        functionName = self.getCollector.__name__
        entrylog(functionName, locals())
        
        node = node.split(".")[0]
        
        result = database.sensorToCollectorMap.get(node, database.sensorToCollectorMap.get('__ALL__'))
        exitlog(functionName, result)
        return result
        
    @agentmethod()
    def getCollectionMetadata(self, msg):
        """
            Request for collector information
        """
        functionName = self.getCollectionMetadata.__name__
        entrylog(functionName, locals())
        
        args = {
            "collectionMetadata" : database.collectionHosts
        }
        call = {'version': 1.0, 'method': 'putCollectionMetadata', 'args': args}
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
        exitlog(functionName)
    
    @agentmethod()
    def putCollectionMetadata(self, msg, collectionMetadata):
        """
            Response for collector information request
        """
        functionName = self.putCollectionMetadata.__name__
        entrylog(functionName, locals())
        
        self.collectionMetadata[msg.src] = CachedItem(msg.src, collectionMetadata)
        queryHash = self.digest("CollectionMetadata", msg.src)
        self.events[queryHash].set()
        self.events[queryHash].clear()
        
        exitlog(functionName)
    
    @agentmethod()
    def ping(self, msg):
        """
            Alive like method call that will send a pong back to the caller
        """
        args = {
            "server": testbed.getNodeName(),
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
        entrylog(functionName, locals())
        m = hashlib.md5()
        for arg in args:
            if type(arg) is set:
                arg = list(arg)
            if type(arg) is list:
                arg.sort()
            m.update(str(arg))
        result = m.hexdigest()
        exitlog(functionName, result)
        return result
    
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

