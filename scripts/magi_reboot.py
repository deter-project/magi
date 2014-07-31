#!/usr/bin/env python

from subprocess import Popen, PIPE
import logging
import optparse
import signal
import sys
import yaml

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def store_list(option, opt_str, value, parser):
    setattr(parser.values, option.dest, value.split(','))
    
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
        
    log.info("Done rebooting MAGI daemon on the required nodes")
    
def getNodesFromAAL(filename):
    nodeSet = set()
    if filename:
        aaldata =  yaml.load(open(filename, 'r')) 
        for name, nodes in aaldata['groups'].iteritems():
            log.info("Adding nodes from group %s", name) 
            nodeSet.update(nodes)
    return nodeSet

def getAllExperimentNodes(project, experiment):
    cmd = "/usr/testbed/bin/node_list -e %s,%s -c" % (project, experiment)
    (output, err) = Popen(cmd.split(), stdout=PIPE).communicate()
    nodeset = set()
    if output:
        for node in output.split('\n')[0].split(' '):
            if node and not node.startswith('tbdelay'):
                nodeset.add(node)
    return nodeset

if __name__ == '__main__':
    optparser = optparse.OptionParser(description="Script to reboot MAGI daemon on experiment nodes") 
    
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
    
    optparser.add_option("-n", "--nodes", dest="nodes", action="callback", callback=store_list, default=[], type="string", 
                         help="Comma-separated list of the nodes to reboot MAGI daemon")
    
    optparser.add_option("-a", "--aal", dest="aal", action="store", default = None, 
                         help="The yaml-based procedure file to extract the list of nodes")
    
    optparser.add_option("-d", "--distpath", dest="distpath", default="/share/magi/current", 
                         help="Location of the distribution") 
    
    optparser.add_option("-U", "--noupdate", dest="noupdate", action="store_true", default=False, 
                         help="Do not update the system before installing Magi")
    
    optparser.add_option("-N", "--noinstall", dest="noinstall", action="store_true", default=False, 
                         help="Do not install magi and the supporting libraries") 
    
    (options, args) = optparser.parse_args()
    if not options.project or not options.experiment:
        optparser.print_help()
        sys.exit(2)

    nodeset = set() 
    if options.aal:
        nodeset = getNodesFromAAL(options.aal) 

    if options.nodes:
        nodeset.update(options.nodes)
        
    if not nodeset:
        nodeset.update(getAllExperimentNodes(options.project, options.experiment))
        
    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL) 

    reboot(options.project, options.experiment, nodeset, options.noupdate, options.noinstall, options.distpath)
    
    sys.exit(0)

