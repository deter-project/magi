#!/usr/bin/env python

from magi.daemon.processInterface import AgentMessenger
from magi.util import config
from magi.util import helpers

import logging
import os
import sys
from magi.messaging.transportPipe import InputPipe, OutputPipe
from magi.messaging.transportTCP import TCPTransport

log = logging.getLogger(__name__)

def initializeProcessAgent(agent, argv):
    '''argv is assumed to have the following format. (This is usually set by the
    Magi daemon):

        agent_name agent_dock execute=[pipe|socket] (logfile=path)

    Where agent_name and agent_dock are strings and the key in the key=value
    pairs is literally the key given. The value may be restricted.
    '''
    if len(argv) < 3:
        log.critical('command line must start with name and dock')
        sys.exit(2)

    agent.name, dock = argv[1:3]
    args = argv_to_dict(argv[3:])
    
    setAttributes(agent, {'hostname' : None, 
                          'execute' : 'socket', 
                          'logfile' : os.path.join('/tmp', agent.name + '.log'), 
                          'loglevel': 'DEBUG', 
                          'commHost': 'localhost', 
                          'commPort': config.getConfig().get('processAgentsCommPort', 18809), 
                          'commGroup': None}, 
                  args)
    
    agent.docklist.add(dock)
    
    handler = logging.FileHandler(agent.logfile, 'w')
    handler.setFormatter(logging.Formatter(helpers.LOG_FORMAT_MSECS, helpers.LOG_DATEFMT))
    root = logging.getLogger()
    root.setLevel(helpers.logLevels.get(agent.loglevel.lower(), logging.INFO))
    root.handlers = []
    root.addHandler(handler)

    log.info('argv: %s', argv)
    log.info('agent attributes: %s', agent.__dict__)
    
    log.info("Setting up agent messaging interface")
    inTransport, outTransport = _getIOHandles(agent)
    agent.messenger = AgentMessenger(inTransport, outTransport, agent)
    agent.messenger.start()
    log.info("Agent messaging interface initialized and running")

    # Tell the daemon we want to listen on the dock. 
    # GTL - why doesn't the Daemon just associate the dock
    # with this process?
    agent.messenger.listenDock(dock)
    
    if agent.commGroup:
        agent.messenger.joinGroup(agent.commGroup)
        #TODO: In ideal condition wait from the node to join group before proceeding further

    # now that we're connected, send an AgentLoaded message. 
    agent.messenger.trigger(event='AgentLoadDone', agent=agent.name, nodes=[agent.hostname])
    
    return args

def argv_to_dict(argv):
    '''Look for key=value pairs and convert them to a dictionary. 
    The set values are always strings. If you want a non-string type, 
    you must coerse it yourself after calling this function.'''
    result = dict()
    for arg in argv:
        words = arg.split('=')
        if len(words) == 2:
            log.debug('found key=value on command line.')
            result[words[0]] = words[1]
    return result
                
def _getIOHandles(agent):

    log.debug("Transport type: %s", agent.execute)
    
    if agent.execute == 'pipe':
        return InputPipe(fileobj=sys.stdin), OutputPipe(fileobj=sys.stdout)
    
    elif agent.execute == 'socket':
        log.debug("Connecting to %s:%d", agent.commHost, agent.commPort)
        transport = TCPTransport(address=agent.commHost, port=agent.commPort)
        return transport, transport
    
    else:
        log.critical('unknown execute mode: %s. Unable to continue.')
        sys.exit(3)

def setAttributes(obj, defaultArgs, args):
    for name, defaultValue in defaultArgs.iteritems():
        setattr(obj, name, args.get(name, defaultValue))
        
