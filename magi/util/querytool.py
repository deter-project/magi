#!/usr/bin/env python

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage
from magi.testbed import testbed
import Queue
import logging
import random
import time
import yaml
import pickle

log = logging.getLogger(__name__)
    
def getData(collectionnames, nodes=None, filters=dict(), timestampChunks=None, bridge='127.0.0.1', msgdest=testbed.nodename, timeout=30):
    """
        Function to fetch data
    """
    functionName = getData.__name__
    entrylog(functionName, locals())
    
    if not collectionnames:
        raise AttributeError("Cannot query for an empty set of collections.")
        
    messenger = getMessenger(bridge, 18808)

    if timestampChunks == None:
        timestampChunks = [(0, time.time())]
    
    log.info("Sending data query message")
    args = {
        "collectionnames": collectionnames,
        "nodes": nodes,
        "filters": filters,
        "timestampChunks": timestampChunks
    }
    call = {'version': 1.0, 'method': 'getData', 'args': args} 
    msg = MAGIMessage(nodes=msgdest, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
    messenger.send(msg)
    log.info("Data query message sent")

    start = time.time()
    stop = start + timeout
    current = start

    result = dict()
    
    # Wait in a loop for timeout seconds 
    while current < stop: 
        try:
            msg = messenger.nextMessage(True, timeout=0.2)
            if msg.src is not srcdock:
                log.info("Received Message")
                log.info("Loading Message")
                if msg.contenttype == MAGIMessage.PICKLE:
                    data = pickle.loads(msg.data)
                elif msg.contenttype == MAGIMessage.YAML:
                    data = yaml.load(msg.data)
                else:
                    raise Exception("Unknown content type")  
                log.info("Message Loaded")
                if 'method' in data and data['method'] == 'putData':
                    log.info("Message is a data query reply")
                    result = data['args']['data']
                    break
                log.info("Message is not a data query reply")
        # If there are no messages in the Queue, just wait some more 
        except Queue.Empty:
            pass 
        current = time.time()
        
    exitlog(functionName)
    return result

msgrCache = dict()
srcdock = "dataclient" + str(int(random.random() * 10000))

def getMessenger(node, port):
    global msgrCache
    if msgrCache.get(node+str(port)) == None:
        msgrCache[node+str(port)] = api.ClientConnection(srcdock, node, port)
    return msgrCache[node+str(port)]

def pingCall(bridge, msgdest):
    """
        Test call to check if data manager agent is available on a given node
    """
    messenger = getMessenger(bridge, 18808)
    call = {'version': 1.0, 'method': 'ping'}
    msg = MAGIMessage(nodes=msgdest, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
    messenger.send(msg)
    msg = messenger.nextMessage(True)
    while True:
        if msg.src == msgdest and msg.dstdocks == 'dataman':
            data = yaml.load(msg.data)
            return data

def getAgentsProcessInfo(node, bridge='127.0.0.1', msgdest=testbed.nodename):
    """
        Function to request process information for active agents on a given node
    """
    messenger = getMessenger(bridge, 18808)
    call = {'version': 1.0, 'method': 'getAgentsProcessInfo'}
    msg = MAGIMessage(nodes=node, docks='daemon', contenttype=MAGIMessage.YAML, data=yaml.dump(call))  
    messenger.send(msg)
    while True:
        msg = messenger.nextMessage(True)
        if msg.src == node:
            data = yaml.load(msg.data)
            return data['result']
        
def entrylog(functionName, arguments):
    log.info("Entering function %s with arguments: %s", functionName, arguments)

def exitlog(functionName, returnValue=None):
    if returnValue:
        log.info("Exiting function %s with return value: %s", functionName, returnValue)
    else:
        log.info("Exiting function %s", functionName)
