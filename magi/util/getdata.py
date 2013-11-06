#!/usr/bin/env python

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage
from magi.testbed import testbed
# TODO: from magi.util import config
from socket import gaierror # this should really be wrapped in daemon lib.
import Queue
import logging
import optparse
import random
import signal
import subprocess
import time
import yaml

log = logging.getLogger(__name__)
    
messaging = None

srcdock = "dataclient" + str(int(random.random() * 10000))

def testCall(bridge, msgdest):
    global messaging
    
    if messaging == None:
        messaging = api.ClientConnection(srcdock, bridge, 18808)
    
    args = {
        "data": "sample data"
    }
    call = {'version': 1.0, 'method': 'test', 'args': args}
    
    msg = MAGIMessage(nodes=msgdest,docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
    messaging.send(msg)

    msg = messaging.nextMessage(True)
    if msg.src is not srcdock:
        data = yaml.load(msg.data)
        return data

def getAgentsProcessInfo(node, bridge='127.0.0.1', msgdest=testbed.nodename):
    global messaging
    if messaging == None:
        messaging = api.ClientConnection(srcdock, bridge, 18808)
    
    call = {'version': 1.0, 'method': 'getAgentsProcessInfo'}
    msg = MAGIMessage(nodes=node, docks='daemon', contenttype=MAGIMessage.YAML, data=yaml.dump(call))  
    messaging.send(msg)
    while True:
        msg = messaging.nextMessage(True)
        if msg.src == node:
            data = yaml.load(msg.data)
            return data['result']

def getData(collectionnames, nodes, timestampChunks=None, bridge='127.0.0.1', msgdest=testbed.nodename, timeout=30):
    functionName = getData.__name__
    entrylog(functionName, locals())
        
    global messaging
    
    nodes = toSet(nodes)
    collectionnames = toSet(collectionnames)
    
    if not nodes:
#        config.topoGraph.nodes()
        pass
    
    if messaging == None:
        messaging = api.ClientConnection(srcdock, bridge, 18808)

    if timestampChunks == None:
        timestampChunks = [(0, time.time())]
    
    count = 0
    
    for node in nodes:
        for collectionname in collectionnames if collectionnames else getCollectionNames(messaging, node):
            sendGetDataMessage(messaging, msgdest, collectionname, node, timestampChunks)
            count += 1

    result = receiveData(messaging, count, timeout)
    
    exitlog(functionName, result)
    return result
   
def getCollectionNames(messaging, node):
    call = {'version': 1.0, 'method': 'getCollectionMetadata'}
    msg = MAGIMessage(nodes=node, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))  
    messaging.send(msg)
    while True:
        msg = messaging.nextMessage(True)
        if msg.src == node:
            data = yaml.load(msg.data)
            collectionMetadata = data['args']['collectionMetadata']
            return collectionMetadata.keys()
        
def sendGetDataMessage(messaging, msgdest, collectionname, node, timestampChunks):
    args = {
        "collectionname": collectionname,
        "node": node,
        "timestampChunks": timestampChunks
    }
    call = {'version': 1.0, 'method': 'getData', 'args': args} 
    
    msg = MAGIMessage(nodes=msgdest, docks='dataman', contenttype=MAGIMessage.YAML, data=yaml.dump(call))
    messaging.send(msg)
        
def receiveData(messaging, count, timeout):
        # Wait for timeout seconds before stopping 
    start = time.time()
    stop = start + timeout
    current = start

    result = dict()
    
    if count == 0:
        return result
    
    # Wait in a loop for timeout seconds 
    while current < stop: 
        try:
            msg = messaging.nextMessage(True, timeout=1)
            if msg.src is not srcdock:
                data = yaml.load(msg.data)
                if 'method' in data and data['method'] == 'putData':
                    collectionname = data['args']['collectionname']
                    node = data['args']['node']
                    if collectionname not in result:
                        result[collectionname] = dict()
                    if node not in result[collectionname]:
                        result[collectionname][node] = []
                    result[collectionname][node] += data['args']['data']
                    count -= 1
                    if count == 0:
                        break
        # If there are no messages in the Queue, just wait some more 
        except Queue.Empty:
            pass 
        current = time.time()
        
    return result

def toSet(value):
    if type(value) is list:
        value = set(value)
    elif type(value) is str:    
        value = set([s.strip() for s in value.split(',')])
    elif value is None:
        value = set()
    
    return value

def entrylog(functionName, arguments):
    log.info("Entering function %s with arguments: %s", functionName, arguments)

def exitlog(functionName, returnValue=None):
    log.info("Exiting function %s with return value: %s", functionName, returnValue)
    
if __name__ == '__main__':
    optparser = optparse.OptionParser()
    optparser.add_option("-b", "--bridge", dest="bridge", help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    optparser.add_option("-n", "--tunnel", dest="tunnel", help="Tell orchestrator to tunnel data through Deter Ops (users.deterlab.net).", default=False, action="store_true")
    optparser.add_option("-t", "--timeout", dest="timeout", default = "30", help="Number of seconds to wait to receive the ping reply from the nodes on the overlay")
    (options, args) = optparser.parse_args()

    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL )

    tun_proc = None
    try:
        if options.tunnel:
            tun_proc = subprocess.Popen("ssh users.deterlab.net -L 18808:" +
                                        options.bridge + ":18808 -N", shell=True)
            bridge = '127.0.0.1'
        else:
            bridge = options.bridge
    except gaierror as e:
        logging.critical('Error connecting to %s: %s', options.control, str(e))
        exit(3)
            
    dbname = "magi"
    collectionname = "log"
    nodes = ["clientnode-2.mongocs1.montage.isi.deterlab.net"]
    
    collectionname = "NodeStatsReporter"
    nodes = ["node1", "node2"]
    nodes = "node1"
    
    timestampChunks = None
    
    msgdest = options.bridge.split(".")[0]
    data = getData(dbname, collectionname, nodes, timestampChunks, bridge, msgdest, int(options.timeout))
    
    if data:
        print (' %s ' % (data))
    else:
        print (' Empty data in message ')
        
    if tun_proc:
        tun_proc.terminate()
        
    exit(0)