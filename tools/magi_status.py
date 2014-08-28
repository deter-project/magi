#!/usr/bin/env python

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage
from magi.util import helpers, config
from subprocess import Popen, PIPE, call
import Queue
import logging
import optparse
import signal
import sys
import time
import yaml
import os

logging.basicConfig(level=logging.INFO, format=helpers.LOG_FORMAT_MSECS, datefmt=helpers.LOG_DATEFMT)
log = logging.getLogger()

def getStatus(project, experiment, nodeSet=set(), groupMembership=False, agentInfo=False, timeout=30):
    
    (bridgeNode, bridgePort) = helpers.getBridge(project=project, experiment=experiment)
    if not nodeSet:
        log.info("Empty node set. Would query for just the bridge node.")
        nodeSet = set(bridgeNode.split('.')[0])
    
    result = dict()
    
    # Join the overlay at the specified bridge node. 
    messaging = api.ClientConnection("ping", bridgeNode, bridgePort)

    
    # Create a ping message and send on the overlay 
    # All node on the overlay will receive it and the daemon will respond with a pong message 
    msg = MAGIMessage(nodes=list(nodeSet), 
                      docks='daemon', 
                      contenttype=MAGIMessage.YAML, 
                      data=yaml.safe_dump({'method': 'getStatus', 
                                           'args': {'groupMembership': groupMembership,
                                                    'agentInfo': agentInfo},
                                           'version': 1.0}))
    messaging.send(msg)

    # Wait for timeout seconds before stopping 
    start = time.time()
    stop = start + int(timeout) 
    current = start

    # Wait in a loop for timeout seconds 
    while current < stop: 
        current = time.time()
        try:
            msg = messaging.nextMessage(True, timeout=1)
            if msg.src is not 'ping':
                log.info('Node %s' %(msg.src))
                result[msg.src] = yaml.load(msg.data)
                nodeSet.discard(msg.src)
        # If there are no messages in the Queue, just wait some more 
        except Queue.Empty:
            #check if there is need to wait any more
            if len(nodeSet) == 0:
                break

    if len(nodeSet) > 0:
        return (False, result)
    
    return (True, result)

def reboot(project, experiment, nodes, noUpdate, noInstall, magiDistDir='/share/magi/current/'):
    rebootProcesses = []
    for node in nodes:
        cmd = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        cmd += "%s.%s.%s sudo %s/magi_bootstrap.py -p %s" % (node, experiment, project, magiDistDir, magiDistDir)
        if noUpdate:
            cmd += ' -U'
        if noInstall:
            cmd += ' -N'
        log.info(cmd)
        p = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        rebootProcesses.append(p)
    
    log.info("Waiting for reboot process to finish")
    for p in rebootProcesses:
        (stdout, stderr) = p.communicate()
        if p.returncode:
            log.error('Exception while rebooting.\n%s'%stderr)
            raise RuntimeError('Exception while rebooting.\n%s'%stderr)
        
    log.info("Done rebooting MAGI daemon on the required nodes")
    
def getLogs(project, experiment, outputdir='/tmp'):
    localLogDir = os.path.join(outputdir, "%s_%s" % (project, experiment))
    helpers.makeDir(localLogDir)
    
    experimentConfigFile = helpers.getExperimentConfigFile(project=project, experiment=experiment)
    experimentConfig = yaml.load(open(experimentConfigFile, 'r'))
    nodeLogDir = experimentConfig.get('expdl', {}).get('nodePaths', {}).get('config', config.DEFAULT_CONF_DIR)
    
    for node in nodeSet:
        localNodeLogDir = os.path.join(localLogDir, node)
        helpers.makeDir(localNodeLogDir)
        cmd = "scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no " \
              "%s.%s.%s:%s %s" % (node, experiment, project, 
                                  os.path.join(nodeLogDir, '*.log'), localNodeLogDir)
        log.info(cmd)
        call(cmd.split())
        
def store_list(option, opt_str, value, parser):
    setattr(parser.values, option.dest, value.split(','))
    

if __name__ == '__main__':
    optparser = optparse.OptionParser(description="Script to get the status of MAGI daemon processes on experiment nodes, \
                                                    to reboot them if required, and to download logs.")
     
#    optparser.add_option("-b", "--bridge", default=None, dest="bridge", 
#                         help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
#    
#    optparser.add_option("-r", "--port", dest="port", type="int", default=18808, 
#                         help="Port on which to contact MAGI daemon on the bridge node")
#    
#    optparser.add_option("-c", "--config", dest="config", help="Experiment configuration file location")
    
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
    
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


    (options, args) = optparser.parse_args()
#    if not options.bridge:
#        if not options.config and (not options.project or not options.experiment):
#            optparser.print_help()
#            sys.exit(2)
    
    if not options.project or not options.experiment:
        optparser.print_help()
        sys.exit(2)

    nodeSet = set() 
    if options.aal:
        nodeSet = helpers.getNodesFromAAL(options.aal) 
    if options.nodes:
        nodeSet.update(options.nodes)
    if not nodeSet:
        nodeSet.update(helpers.getExperimentNodeList(project=options.project, 
                                                     experiment=options.experiment))
        
    if options.logs:
        getLogs(options.project, options.experiment, options.logoutdir)
        exit(0)
        
    if options.reboot:
        if not options.distpath:
            experimentConfigFile = helpers.getExperimentConfigFile(project=options.project,
                                                     experiment=options.experiment)
            experimentConfig = yaml.load(open(experimentConfigFile, 'r'))
            distributionPath = experimentConfig.get('expdl', {}).get('distributionPath', config.DEFAULT_DIST_DIR)
        else:
            distributionPath = options.distpath
        reboot(options.project, options.experiment, nodeSet, options.noupdate, options.noinstall, distributionPath)
        log.info("Waiting for transports to be setup")
        time.sleep(20)

    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL) 

    (status, result) = getStatus(project=options.project, 
                                 experiment=options.experiment, 
                                 nodeSet=nodeSet, 
                                 groupMembership=options.groupmembership,
                                 agentInfo=options.agentinfo,
                                 timeout=options.timeout)

    log.info("Result:\n%s" %(yaml.dump(result)))
    
    if not status:
        log.info("Did not receive reply from %s", sorted(list(nodeSet-set(result.keys()))))
    else:
        log.info("Received reply back from all the required nodes")
