#!/usr/bin/env python

from subprocess import Popen, PIPE
import yaml
import os

LOG_FORMAT = '%(asctime)s %(name)-30s %(levelname)-8s %(message)s'
LOG_FORMAT_MSECS = '%(asctime)s.%(msecs)03d %(name)-30s %(levelname)-8s %(message)s'
LOG_DATEFMT = '%m-%d %H:%M:%S'

def getNodesFromAAL(filename):
    nodeSet = set()
    if filename:
        aaldata = yaml.load(open(filename, 'r')) 
        for name, nodes in aaldata['groups'].iteritems():
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
   
def getExperimentConfigFile(project, experiment):
    configFile = os.path.join('/proj', project, 'exp', experiment, 'experiment.conf')
    return configFile

def getBridge(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
    
    mesdl = yaml.load(open(experimentConfigFile, 'r'))['mesdl']
    bridges = mesdl['bridges']
    
    return (bridges[0]['server'], bridges[0]['port'])

def getDBConfigHost(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
    
    experimentConfig = yaml.load(open(experimentConfigFile, 'r'))
    dbdl = experimentConfig['dbdl']
    expdl = experimentConfig['expdl']
    
    return "%s.%s.%s" % (dbdl['configHost'], expdl['eid'], expdl['pid'])

def load_yaml_idl(agent_name,agent_procedure):
    agent_aal = load_yaml(agent_procedure)
    try:
        tar = tarfile.open(agent_aal['agents'][agent_name]['path'])
        for member in tar.getmembers():
            if member.name.endswith('.idl'):
                f = tar.extractfile(member)
                content = f.read()
                config = yaml.load(content)
                return config
    except IOError as e:
        logging.critical("File not found: %s", str(e))
        sys.exit(2)
        
def load_yaml(file_aal):
   try:
		f=open(file_aal)  
		config=yaml.load(f)
        	return config
   except IOError as e:
		logging.critical("File not found: %s", str(e))
		sys.exit(2)
