#!/usr/bin/env python

import asyncore
import logging
import signal
from subprocess import Popen

from magi.daemon.processInterface import AgentCodec, AgentRequest
from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage, DefaultCodec
from magi.messaging.transportTCP import TCPServer, TCPTransport
import yaml


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

mainfile = '/users/rnarasim/dec/testmod'
agentName = 'cAgent'
agentDock = 'cAgent_dock'
methodCall = {'method': "ccat", 
              'args': {'key1' : 'ro','key2':'hit'},
              'version': 1.0,
              'trigger':'dictResult'}
              
def directTest():

    server = TCPServer(address="0.0.0.0", port=18811)
    pollMap = dict()
    pollMap[server.fileno()] = server
    
    args = []
    args.append('hostname=testServer')
    args.append('execute=socket')
    args.append('commHost=clientnode')
    args.append('commPort=18811')
    args.append('logfile=/users/jaipuria/playground/deter/magi/scripts/cAgent.log')   	
    logging.debug('running: %s', ' '.join([mainfile, agentName, agentDock] + args))
    agent = Popen([mainfile, agentName, agentDock] + args, close_fds=True)
                    
    outMsg = MAGIMessage(nodes="nodes", 
                      docks=agentDock, 
                      contenttype=MAGIMessage.YAML, 
                      data=yaml.safe_dump(methodCall))

    agentCodec = AgentCodec()
    msgCodec = DefaultCodec()
    
    while True:
        
        asyncore.poll(1, pollMap)
        
        for fd, transport in pollMap.items():
            
                print transport
                print len(transport.inmessages)
                
                if len(transport.inmessages) > 0:
                    log.debug("%d messages from %s", len(transport.inmessages), transport)
    
                for obj in transport.inmessages:
                    
                    if isinstance(obj, TCPTransport):
                        log.info("New TCP connection made from an agent: %s", obj)
                        pollMap[obj.fileno()] = obj  # docks come later in requests
                        obj.setCodec(agentCodec)
                    
                    elif isinstance(obj, AgentRequest):
                        
                        if obj.request == AgentRequest.MESSAGE:
                            log.debug("%s requests send message" % (transport))
                            msg, hdrsize = msgCodec.decode(obj.data)
                            msg.data = obj.data[hdrsize:]
                            log.info("MAGI Message : %s" %msg)
                            log.info("MAGI Message Data START:%s:END" %str(msg.data))
                            log.info("Ya")
                            log.info(yaml.load(str(msg.data)))
                            
                        else:
                            log.info(obj.__dict__)
                            log.info("Type = %s, Data = %s" %(obj.request, obj.data))
                            
                            log.debug("Sending msg: %s", outMsg)
                            request = AgentRequest.MAGIMessage(outMsg)
    #                        request = AgentRequest(request=AgentRequest.LEAVE_GROUP, data='dummyGrp')
                            transport.outmessages.append(request)
                            
                    else:
                        log.error("Unknown request from agent.")
                        log.error(obj)
                        
                transport.inmessages = []
    
def inDirectTest():
    bridge = 'clientnode.clientserver.montage'
    port = 18808
    messaging = api.ClientConnection("testCAgent", bridge, port)
    messaging.join('control')
    
    log.info('connection done')
    
    args = {
            "name": agentName,
            "code": 'cAgent_code',
            "dock": agentDock,
            "execargs": ''
        }
    args["path"] = "/users/rnarasim/dec/"


    call = {'version': 1.0, 'method': 'loadAgent', 'args': args}
    msg = MAGIMessage(nodes='clientnode', docks='daemon', 
                        contenttype=MAGIMessage.YAML, 
                        data=yaml.dump(call))
    
    log.info("sending msg %s" %msg)
    messaging.send(msg)
    while True:
        try:
            msg = messaging.nextMessage(True, 1.0)
        except:
            continue
        break
    
    log.info("rcvd msg")
    log.info(msg)

    msg = MAGIMessage(nodes='clientnode', 
                      docks=agentDock, 
                      contenttype=MAGIMessage.YAML, 
                      data=yaml.safe_dump(methodCall))
    messaging.send(msg)
    log.info("sending msg %s" %msg)
    
    while True:
        try:
            msg = messaging.nextMessage(True, 1.0)
        except:
            continue
        break
    log.info("rcvd msg2")
    log.info(msg)
    
    call = {'version': 1.0, 'method': 'unloadAgent', 'args': args}
    msg = MAGIMessage(nodes='clientnode', docks='daemon', 
                        contenttype=MAGIMessage.YAML, 
                        data=yaml.dump(call))
    
    messaging.send(msg)
    while True:
        try:
            msg = messaging.nextMessage(True, 1.0)
        except:
            continue
        break
    log.info("rcvd msg3")
    log.info(msg)
    
    #msg = messaging.nextMessage(True)
    #print msg.data
    
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
#    directTest()
    inDirectTest()
