#!/usr/bin/env python

from magi.messaging.magimessage import MAGIMessage
from magi.testbed import testbed
from magi.util import database, config
from magi.util.agent import NonBlockingDispatchAgent

import itertools
import networkx as nx
import time

import logging
import yaml
import threading

import hashlib

log = logging.getLogger(__name__)

def getAgent():
        return DataManAgent()


class DataManAgent(NonBlockingDispatchAgent):
    
    def __init__(self, *args, **kwargs):
        NonBlockingDispatchAgent.__init__(self, *args, **kwargs)
        
        self.topoGraph = testbed.getTopoGraph()
        self.collectorMapping = config.getConfig().get('collector_mapping')
        self.collectors = self.collectorMapping.values()
        self.queriers = config.getConfig().get('queriers')
        self.cacheSet = self.calculateCacheSet()
        
        self.collectionMetadata= dict()
        self.distanceCache = dict()
        self.dataShelf = dict()
        
        self.events = dict()
        
        
    def test(self, msg, *args):
        args = {
            "server": testbed.getNodeName(),
            "result": "success"
        }
        call = {'version': 1.0, 'method': 'test', 'args': args}
        msg = MAGIMessage(nodes=msg.src, groups=['__ALL__'], docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
    def getCollectionMetadata(self, msg):
        functionName = self.getCollectionMetadata.__name__
        entrylog(functionName, locals())
        
        args = {
            "collectionMetadata" : database.collectionHosts
        }
        call = {'version': 1.0, 'method': 'putCollectionMetadata', 'args': args}
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
        exitlog(functionName)
        
    def putCollectionMetadata(self, msg, collectionMetadata):
        functionName = self.putCollectionMetadata.__name__
        entrylog(functionName, locals())
        
        self.collectionMetadata[msg.src] = CachedItem(msg.src, collectionMetadata)
        queryHash = self.digest("CollectionMetadata", msg.src)
        self.events[queryHash].set()
        self.events[queryHash].clear()
        
        exitlog(functionName)
        
    def getData(self, msg, collectionname, node, filters=dict(), timestampChunks=None, visited=set()):
        functionName = self.getData.__name__
        entrylog(functionName, locals())
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
            
        data = self.getDataInternal(collectionname, node, filters, timestampChunks, visited)
        args = {
            "collectionname": collectionname,
            "node": node,
            "filters": filters,
            "timestampChunks": timestampChunks,
            "visited": visited,
            "data": data
        }
        call = {'version': 1.0, 'method': 'putData', 'args': args}
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
        exitlog(functionName)
        
    def getDataInternal(self, collectionname, node, filters=dict(), timestampChunks=None, visited=set()):
        functionName = self.getDataInternal.__name__
        entrylog(functionName, locals())

        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        data = []
        chunksNotCached = []
        
        if database.isDBHost:
            log.debug("Database server is hosted locally")
            
            filters_copy = filters.copy()
            filters_copy['host'] = node
                
            for tsChunk in timestampChunks:
                data = data + database.getData(collectionname, filters_copy, tsChunk)
        
            if self.isCollector(node, collectionname):
                log.debug("This node is the collector for: %s:%s", node, collectionname)
                return data
            elif self.isCache(node, collectionname):
                log.debug("This node is the cache for: %s:%s", node, collectionname)
                for tsChunk in timestampChunks:
                    chunksNotCached = chunksNotCached + database.findTimeRangeNotAvailable(collectionname, filters_copy, tsChunk)
            else:
                chunksNotCached = timestampChunks
        else:
            log.debug("Database server is not hosted locally")
            chunksNotCached = timestampChunks
            
        log.debug("Chunks not available locally: " + str(chunksNotCached))
        if chunksNotCached:
            data = data + self.getDataFromNeighbor(collectionname, node, filters, chunksNotCached, visited)
        
        exitlog(functionName, data)
        return data

    def getDataFromNeighbor(self, collectionname, node, filters=dict(), timestampChunks=None, visited=set()):
        functionName = self.getDataFromNeighbor.__name__
        entrylog(functionName, locals())
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        neighbor = self.getShortestDistanceInternal(collectionname, node, filters, timestampChunks, visited)[1]
        if not neighbor:
            log.debug("No neighbor to get required data from")
            data = []
            exitlog(functionName, data)
            return data
        
        log.debug("Neighbor to get data from: " + neighbor)
        visited_copy = visited.copy()
        visited_copy.add(testbed.getNodeName())
        args = {
            "collectionname": collectionname,
            "node": node,
            "filters": filters,
            "timestampChunks": timestampChunks,
            "visited": visited_copy
        }
        call = {'version': 1.0, 'method': 'getData', 'args': args}
        querymsg = MAGIMessage(nodes=neighbor, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        
        queryHash = self.digest(collectionname, node, filters, timestampChunks, visited_copy)
        log.debug("getData Query Hash: " + queryHash)
        
        self.events[queryHash] = threading.Event()
        
        self.messenger.send(querymsg)
        
        self.events[queryHash].wait()
        #while queryHash not in self.dataShelf.keys():
        #    continue

        data = self.dataShelf[queryHash]
        del self.dataShelf[queryHash]
        
        exitlog(functionName, data)
        return data
        
    def putData(self, msg, collectionname, node, filters=dict(), timestampChunks=None, visited=set(), data=[]):
        functionName = self.putData.__name__
        entrylog(functionName, locals())
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        queryHash = self.digest(collectionname, node, filters, timestampChunks, visited)
        log.debug("putData Query Hash: " + queryHash)
        
        self.dataShelf[queryHash] = data
        self.events[queryHash].set()
        
        if self.isCache(node, collectionname):
            filters['host'] = node
            database.updateDatabase(collectionname, filters, timestampChunks, data)
    
    def getShortestDistance(self, msg, collectionname, node, filters=dict(), timestampChunks=None, visited=set()):
        functionName = self.getShortestDistance.__name__
        entrylog(functionName, locals())
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        result = self.getShortestDistanceInternal(collectionname, node, filters, timestampChunks, visited)
        args = {
            "collectionname": collectionname,
            "node": node,
            "filters": filters,
            "timestampChunks": timestampChunks,
            "visited": visited,
            "distance": result[0]
        }
        call = {'version': 1.0, 'method': 'putShortestDistance', 'args': args}
        msg = MAGIMessage(nodes=msg.src, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        self.messenger.send(msg)
        
    
    def getShortestDistanceInternal(self, collectionname, node, filters=dict(), timestampChunks=None, visited=set(), checkLocalCache=True):
        functionName = self.getShortestDistanceInternal.__name__
        entrylog(functionName, locals())
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        cacheHash = self.digest(collectionname, node, filters)
        log.debug("Cache Hash: " + cacheHash)
        
        if cacheHash in self.distanceCache and self.distanceCache[cacheHash].isValid():
            log.debug("Shortest distance value available in cache")
            result = self.distanceCache[cacheHash].value
            return result
        
        if checkLocalCache:
            if self.isCollector(node, collectionname):
                log.debug("This node is the collector for: " + node)
                result = 0, None
                return result
            elif self.isCache(node, collectionname):
                log.debug("This node is a cache for: " + node)
                chunksNotCached = []
                filters_copy = filters.copy()
                filters_copy['host'] = node
                for tsChunk in timestampChunks:
                    chunksNotCached = chunksNotCached + database.findTimeRangeNotAvailable(collectionname, filters_copy, tsChunk)
            else:
                chunksNotCached = timestampChunks
        else:
            chunksNotCached = timestampChunks
            
        log.debug("Chunks not available locally: " + str(chunksNotCached))
        if chunksNotCached == []:
            result = 0, None
            return result
    
        visited_copy = visited.copy()
        visited_copy.add(testbed.getNodeName())
        neighbors = self.topoGraph.neighbors(testbed.getNodeName())
        neighborsNotVisited = set(neighbors) - set(visited_copy)
        
        collector = self.getCollector(node, collectionname)
        if not collector:
            log.debug("No collector for %s:%s", node, collectionname)
            result = 999, None
            return result
                    
        nodesOnPath = self.findNodesOnPath(self.topoGraph, testbed.nodename, collector)
        
        neighborsToVisit = neighborsNotVisited.intersection(nodesOnPath)
        
        log.debug("Neighbors to visit: " + str(neighborsToVisit))
        if not neighborsToVisit:
            result = 999, None
            return result
        
        args = {
            "collectionname": collectionname,
            "node": node,
            "filters": filters,
            "timestampChunks": chunksNotCached,
            "visited": visited_copy
        }
        call = {'version': 1.0, 'method': 'getShortestDistance', 'args': args}
        querymsg = MAGIMessage(nodes=neighborsToVisit, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        
        queryHash = self.digest(collectionname, node, filters, chunksNotCached, visited_copy)
        log.debug("getShortestDistance Query Hash: " + queryHash)

        queryWaitSet = set()
        for neighbor in neighborsToVisit:
            queryWaitSet.add(self.digest(queryHash, neighbor))
                    
        for entry in queryWaitSet:
            #TODO: 
            self.events[entry] = threading.Event()
        
        self.messenger.send(querymsg)
        
        for entry in queryWaitSet:
            self.events[entry].wait()
        
        totalReqd = 0
        for tsChunk in timestampChunks:
            totalReqd = totalReqd + (tsChunk[1] - tsChunk[0])
        
        totalNotCached = 0
        for chunk in chunksNotCached:
            totalNotCached = totalNotCached + (chunk[1] - chunk[0])
        
        percentNotAvailable = float(totalNotCached)/totalReqd
        
        if cacheHash in self.distanceCache and self.distanceCache[cacheHash].isValid():
            result = percentNotAvailable * self.distanceCache[cacheHash].value[0], self.distanceCache[cacheHash].value[1]
        else:
            result = 999, None
            
        return result
    
            
    def putShortestDistance(self, msg, collectionname, node, filters=dict(), timestampChunks=None, visited=set(), distance=999):
        functionName = self.putShortestDistance.__name__
        entrylog(functionName, locals())
        
        if timestampChunks == None:
            timestampChunks = [(0, time.time())]
        
        reporterNode = msg.src
        log.debug("Reporter node: " + reporterNode)
        
        distance = distance + nx.dijkstra_path_length(self.topoGraph, testbed.getNodeName(), reporterNode)
        log.debug("Distance: " + str(distance))
        
        cacheHash = self.digest(collectionname, node, filters)
        log.debug("Cache Hash: " + cacheHash)
        
        if cacheHash in self.distanceCache and self.distanceCache[cacheHash].isValid():
            cachedDistance = self.distanceCache[cacheHash].value[0]
            if distance < cachedDistance:
                self.distanceCache[cacheHash] = CachedItem(cacheHash, (distance, reporterNode))
        else:
            self.distanceCache[cacheHash] = CachedItem(cacheHash, (distance, reporterNode))
    
        queryHash = self.digest(collectionname, node, filters, timestampChunks, visited)
        log.debug("putShortestDistance Query Hash: " + queryHash)
        
        key = self.digest(queryHash, reporterNode)
        if key in self.events:
            self.events[key].set()
 
    def getCollector(self, node, collectionname):
        functionName = self.getCollector.__name__
        entrylog(functionName, locals())
        
        node = node.split(".")[0]
#        return self.collectorMapping[node]
        if node == testbed.nodename:
            log.debug("Querying for local node")
            if collectionname in database.collectionHosts:
                return database.collectionHosts[collectionname]
            else:
                return None
            
        if node in self.collectionMetadata:
            if collectionname in self.collectionMetadata[node].value:
                log.debug("Info available in cache")
                return self.collectionMetadata[node].value[collectionname]
            elif self.collectionMetadata[node].isValid():
                log.debug("Info available in cache")
                return None
        
        call = {'version': 1.0, 'method': 'getCollectionMetadata'}
        querymsg = MAGIMessage(nodes=node, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
        queryHash = self.digest("CollectionMetadata", node)
        log.debug("getCollectionMetadata Query Hash: " + queryHash)
        self.events[queryHash] = threading.Event()
        self.messenger.send(querymsg)
        self.events[queryHash].wait()
        
        if collectionname in self.collectionMetadata[node].value:
            return self.collectionMetadata[node].value[collectionname]
        else:
            return None
       
    def isCollector(self, node, collectionname):
        return (self.getCollector(node, collectionname) == testbed.getNodeName())
                
    def isCache(self, node, collectionname):
        node = node.split(".")[0]
        collector = self.getCollector(node, collectionname)
        return collector in self.cacheSet
    
    def calculateCacheSet(self):
        result = set()
        
        if not database.isDBHost:
            return result
        
        if self.queriers == None:
            return result
        
        if testbed.getNodeName() in self.queriers:
            result.update(set(self.collectors))
            return result
        
        for collector in self.collectors:
            if self.findIfPossibleCache(self.topoGraph, self.queriers, collector, testbed.getNodeName()):
                result.add(collector)
        return result
    
    def findIfPossibleCache(self, graph, starts, end, node):
    
        paths = dict()
        for start in starts:
            paths[start] = list(nx.all_simple_paths(graph, start, end))
        
        for pair in list(itertools.combinations(starts, 2)):
            paths1 = paths[pair[0]]
            paths2 = paths[pair[1]]
        
            for path1 in paths1:
                for path2 in paths2:
                    if self.findIfIntersectionPoint(path1, path2, node):
                        return True
                
        return False
        
    def findIfIntersectionPoint(self, path1, path2, node):
        if node not in path1 or node not in path2:
            return False
    
        if node in self.findFirstIntersectionPoint(path1, path2):
            return True
    
        return False
    
    def findIntersectionPoints(self, graph, start1, start2, end):
        result = set()
    
        paths1 = list(nx.all_simple_paths(graph, start1, end))
        paths2 = list(nx.all_simple_paths(graph, start2, end))
    
        for path1 in paths1:
            for path2 in paths2:
                result.update(self.findFirstIntersectionPoint(path1, path2))
    
        return result


    def findFirstIntersectionPoint(self, path1, path2):
        result = set()
    
        for node in path1:
            if node in path2:
                result.add(node)
                break
    
        for node in path2:
            if node in path1:
                result.add(node)
                break
    
        return result

    def findNodesOnPath(self, graph, start, end):
        result = set()
        paths = list(nx.all_simple_paths(graph, start, end))
        for path in paths:
            for node in path:
                result.add(node)
        
        return result
            
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
    
def entrylog(functionName, arguments):
    log.debug("Entering function %s with arguments: %s", functionName, arguments)

def exitlog(functionName, returnValue=None):
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

