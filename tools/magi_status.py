#!/usr/bin/env python

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage
from magi.util import helpers
import Queue
import base64
import cStringIO
import logging
import optparse
import os
import signal
import tarfile
import tempfile
import time
import yaml

logging.basicConfig(level=logging.INFO, format=helpers.LOG_FORMAT_MSECS, datefmt=helpers.LOG_DATEFMT)
log = logging.getLogger()

CLIENT_NAME = "magiStatusTool"

def getStatus(bridgeNode, bridgePort, nodeSet=set(), groupMembership=False, agentInfo=False, timeout=30):
    
    if not nodeSet:
        log.info("Empty node set. Would query for just the bridge node.")
        nodeSet = set([bridgeNode.split('.')[0]])
    
    log.info("Node Set: %s" %(nodeSet))
    
    args = {'groupMembership': groupMembership,
            'agentInfo': agentInfo}
    messaging = sendMessage(bridgeNode, bridgePort, list(nodeSet), 'daemon', 'getStatus', args)
    
    result = recieveMessages(messaging, nodeSet, timeout)

    failedNodes = nodeSet - set(result.keys())
    return ((len(failedNodes) == 0), result)

def reboot(bridgeNode, bridgePort, nodeSet, magiDistDir=None, noUpdate=False, noInstall=False, timeout=30):

    if not nodeSet:
        log.info("Empty node set. Would query for just the bridge node.")
        nodeSet = set([bridgeNode.split('.')[0]])
    
    log.info("Node Set: %s" %(nodeSet))
    
    args = {'distributionDir': magiDistDir,
            'noUpdate': noUpdate, 
            'noInstall': noInstall}
    messaging = sendMessage(bridgeNode, bridgePort, list(nodeSet), 'daemon', 'reboot', args)
    
    log.info("Done sending reboot messages to MAGI daemons on the required nodes")
    
    recieveMessages(messaging, nodeSet, timeout=5)
            
def getLogsArchive(bridgeNode, bridgePort, nodeSet=set(), outputdir='/tmp', timeout=30):
    
    if not nodeSet:
        log.info("Empty node set. Would query for just the bridge node.")
        nodeSet = set([bridgeNode.split('.')[0]])
    
    log.info("Node Set: %s" %(nodeSet))
        
    messaging = sendMessage(bridgeNode, bridgePort, list(nodeSet), 'daemon', 'getLogsArchive', {})

    result = recieveMessages(messaging, nodeSet, timeout)

    helpers.makeDir(outputdir)
    
    for node in result:
        nodeLogDir = os.path.join(outputdir, node)
        tardata = result[node]
        scratch = tempfile.TemporaryFile()
        sp = cStringIO.StringIO(tardata)
        base64.decode(sp, scratch)
        sp.close()

        # now untar that into the output directory
        scratch.seek(0)
        tar = tarfile.open(fileobj=scratch, mode="r:gz")
        for m in tar.getmembers():
            tar.extract(m, nodeLogDir)
        tar.close()
        
    failedNodes = nodeSet - set(result.keys())
    return ((len(failedNodes) == 0), result.keys())

def sendMessage(bridgeNode, bridgePort, nodes, docks, method, args):
    
    # Join the overlay at the specified bridge node. 
    messaging = api.ClientConnection(CLIENT_NAME, bridgeNode, bridgePort)
    
    # Create a ping message and send on the overlay 
    # All node on the overlay will receive it and the daemon will respond with a pong message 
    msg = MAGIMessage(nodes=nodes, 
                      docks=docks, 
                      contenttype=MAGIMessage.YAML, 
                      data=yaml.safe_dump({'method': method, 
                                           'args': args,
                                           'version': 1.0}))
    log.debug("Sending msg: %s" %(msg))
    messaging.send(msg)
    
    return messaging

def recieveMessages(messaging, nodeSet, timeout=30):
    
    result = dict()
    nodes = helpers.toSet(value=nodeSet.copy())
    
    # Wait for timeout seconds before stopping 
    start = time.time()
    stop = start + int(timeout) 
    current = start

    # Wait in a loop for timeout seconds 
    while current < stop: 
        current = time.time()
        try:
            msg = messaging.nextMessage(True, timeout=1)
            log.debug(msg)
            if msg.src is not CLIENT_NAME:
                log.info('Node %s' %(msg.src))
                result[msg.src] = yaml.load(msg.data)
                nodes.discard(msg.src)
        # If there are no messages in the Queue, just wait some more 
        except Queue.Empty:
            #check if there is need to wait any more
            if len(nodes) == 0:
                break
            
    return result

def store_list(option, opt_str, value, parser):
    setattr(parser.values, option.dest, value.split(','))
    

if __name__ == '__main__':
    optparser = optparse.OptionParser(description="Script to get the status of MAGI daemon processes on experiment nodes, \
                                                    to reboot them if required, and to download logs.")
     
    optparser.add_option("-b", "--bridge", default=None, dest="bridge", 
                         help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    
    optparser.add_option("-p", "--port", dest="port", type="int", default=18808, 
                         help="The port to connect to on the bridge node")
    
    optparser.add_option("-c", "--config", dest="config", help="Experiment configuration file location")
    
    optparser.add_option("-n", "--nodes", dest="nodes", action="callback", callback=store_list, default=[], type="string", 
                         help="Comma-separated list of the nodes to reboot MAGI daemon")
    
    optparser.add_option("-a", "--aal", dest="aal", action="store", default = None, 
                         help="The yaml-based procedure file to extract the list of nodes")
    
    optparser.add_option("-l", "--logs", dest="logs", action="store_true", default=False, 
                         help="Fetch logs. The -o/--logoutdir option is applicable only when fetching logs.")
    
    optparser.add_option("-o", "--logoutdir", dest="logoutdir", default='/tmp',
                         help="Store logs under the given directory. Default: %default")

    optparser.add_option("-g", "--groupmembership", dest="groupmembership", action="store_true", default=False, 
                         help="Fetch group membership detail")
    
    optparser.add_option("-i", "--agentinfo", dest="agentinfo", action="store_true", default=False, 
                         help="Fetch loaded agent information")
    
    optparser.add_option("-t", "--timeout", dest="timeout", default = 10, 
                         help="Number of seconds to wait to receive the status reply from the nodes on the overlay")
        
    optparser.add_option("-r", "--reboot", dest="reboot", action="store_true", default=False, 
                         help="Reboot nodes. The following options are applicable only when rebooting.") 
    
    optparser.add_option("-d", "--distpath", dest="distpath", help="Location of the distribution") 
    
    optparser.add_option("-U", "--noupdate", dest="noupdate", action="store_true", default=False, 
                         help="Do not update the system before installing MAGI")
    
    optparser.add_option("-N", "--noinstall", dest="noinstall", action="store_true", default=False, 
                         help="Do not install MAGI and the supporting libraries")

    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL) 

    (options, args) = optparser.parse_args()
    if options.bridge:
        bridgeNode = options.bridge
        bridgePort = options.port
    elif options.config:
        (bridgeNode, bridgePort) = helpers.getBridge(experimentConfigFile=options.config)
    else:
        optparser.print_help()
        optparser.error("Missing bridge and "
                        "experiment configuration file")
            
    nodeSet = set() 
    if options.nodes:
        nodeSet = helpers.toSet(options.nodes)
    if options.aal:
        nodeSet.update(helpers.getNodesFromAAL(options.aal))
    if not nodeSet and options.config:
        nodeSet.update(helpers.getExperimentNodeList(experimentConfigFile=options.config))
        
    if options.logs:
        (status, result) = getLogsArchive(bridgeNode=bridgeNode, 
                                          bridgePort=bridgePort, 
                                          nodeSet=nodeSet, 
                                          outputdir=options.logoutdir)
        log.info("Received logs stored under %s" %(options.logoutdir))
        exit(0)
        
    if options.reboot:
        distributionPath = None
        if options.distpath:
            distributionPath = options.distpath
        elif options.config:
            experimentConfig = yaml.load(open(options.config, 'r'))
            distributionPath = experimentConfig.get('expdl', {}).get('distributionPath')
        
        reboot(bridgeNode=bridgeNode, 
               bridgePort=bridgePort, 
               nodeSet=nodeSet, 
               magiDistDir=distributionPath, 
               noUpdate=options.noupdate, 
               noInstall=options.noinstall)
        exit(0)

    (status, result) = getStatus(bridgeNode=bridgeNode, 
                                 bridgePort=bridgePort, 
                                 nodeSet=nodeSet, 
                                 groupMembership=options.groupmembership,
                                 agentInfo=options.agentinfo,
                                 timeout=options.timeout)

    log.info("Result:\n%s" %(yaml.dump(result)))
    
    if not status:
        log.info("Did not receive reply from %s", sorted(list(nodeSet-set(result.keys()))))
    else:
        log.info("Received reply back from all the required nodes")
