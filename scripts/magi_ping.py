#!/usr/bin/env python

import Queue 
import time 
import yaml
import optparse
import signal
import sys
import logging 

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def ping(bridgeNode, bridgePort, nodeSet=set(), timeout=30):
    
    if nodeSet:
        checkOnlyNodeSet = True
    else:
        checkOnlyNodeSet = False
    
    # Join the overlay at the specified bridge node. 
    messaging = api.ClientConnection("ping", bridgeNode, bridgePort)

    # Create a ping message and send on the overlay 
    # All node on the overlay will receive it and the daemon will respond with a pong message 
    msg = MAGIMessage(groups=['__ALL__'], docks='daemon', contenttype=MAGIMessage.YAML, data=yaml.safe_dump({'method': 'ping', 'version': 1.0}))
    messaging.send(msg)

    # Wait for timeout seconds before stoppping 
    start = time.time()
    stop = start + int(timeout) 
    current = start

    # Wait in a loop for timeout seconds 
    while current < stop: 
        current = time.time()
        try:
            msg = messaging.nextMessage(True, timeout=1)
            if msg.src is not 'ping':
                log.info('%s: Node %s on magi overlay' % ( current, msg.src))
                nodeSet.discard(msg.src)
        # If there are no messages in the Queue, just wait some more 
        except Queue.Empty:
            #check if there is need to wait any more
            if checkOnlyNodeSet and len(nodeSet) == 0:
                break

    if len(nodeSet) > 0:
        return (False, nodeSet)
    
    return (True, nodeSet)

def getNodesFromAAL(filename):
    nodeSet = set()
    if filename:
        aaldata =  yaml.load(open(filename, 'r')) 
        for name, nodes in aaldata['groups'].iteritems():
            log.info("Adding nodes from group %s", name) 
            nodeSet.update(nodes)
    return nodeSet
            
if __name__ == '__main__':
    optparser = optparse.OptionParser(description="Program to check if MAGI daemon is up and listening on the required node \
                                                    Bridge Node OR Experiment Configuration File OR Project and Experiment Name \
                                                    needs to be provided to be able to connect to the experiment.")
     
    optparser.add_option("-b", "--bridge", default=None, dest="bridge", 
                         help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    
    optparser.add_option("-r", "--port", dest="port", type="int", default=18808, 
                         help="Port on which to contact MAGI daemon on the bridge node")
    
    optparser.add_option("-c", "--config", dest="config", help="Experiment configuration file location")
    
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
    
    optparser.add_option("-a", "--aal", dest="aal", action="store", default=None, 
                         help="The yaml-based procedure file. If one is provided, the list of interested nodes is calculated based on it.")
    
    optparser.add_option("-t", "--timeout", dest="timeout", default = "5", 
                         help="Number of seconds to wait to receive the ping reply from the nodes on the overlay")

    (options, args) = optparser.parse_args()
    if not options.bridge:
        if not options.config and (not options.project or not options.experiment):
            optparser.print_help()
            sys.exit(2)
        from magi_get_config import getBridge
        (options.bridge, options.port) = getBridge(experimentConfigFile=options.config, project=options.project, experiment=options.experiment)

    nodeset = set() 
    if options.aal is not None:
        nodeset = getNodesFromAAL(options.aal)

    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL) 

    (result, remainingNodes) = ping(options.bridge, options.port, nodeset, options.timeout)

    if not result:
        log.info("Did not receive reply from %s", sorted(list(remainingNodes)))
    
    elif options.aal is not None:
        log.info("Received reply back from all the nodes referenced in the given AAL file")

