from magi.testbed import testbed
from subprocess import Popen, PIPE
import Queue
import ctypes
import errno
import logging
import os
import platform
import yaml
import tarfile

log = logging.getLogger(__name__)

logLevels = {
        'NONE': 100,
        'ALL': 0,
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

LOG_FORMAT = '%(asctime)s %(name)-30s %(levelname)-8s %(message)s'
LOG_FORMAT_MSECS = '%(asctime)s.%(msecs)03d %(name)-30s %(levelname)-8s %(message)s'
LOG_DATEFMT = '%m-%d %H:%M:%S'

ALL = '__ALL__'

def makeDir(name):
    try:
        os.makedirs(name)
    except OSError, e:
        if e.errno == errno.EEXIST: return
        log.warning("Couldn't create directory: %s", name)
        raise

def makePipe(name):
    try:
        os.mkfifo(name)
    except OSError, e:
        if e.errno == errno.EEXIST: return
        log.warning("Couldn't create FIFO file: %s", name)
        raise
        
def loadYaml(filename):
    """ Load the configuration data from file """
    fp = open(filename, 'r')
    data = yaml.load(fp)
    fp.close()
    return data

def readPropertiesFile(filename):
    import ConfigParser
    import io
    parser = ConfigParser.RawConfigParser()
    properties = '[root]\n' + open(filename, 'r').read()
    parser.readfp(io.BytesIO(properties))
    kv_pairs = parser.items('root')
    return dict(kv_pairs)

def toSet(value):
    if type(value) is list:
        value = set(value)
    elif type(value) is str:    
        value= set([s.strip() for s in value.split(',')])
    elif value is None:
        value= set()
    return value

def toDirected(graph, root):
    """
        Convert an undirected graph to a directed graph
    """
    import networkx as nx
    
    d = nx.DiGraph()
    queue = Queue.Queue()
    visited = set()
    
    queue.put(root)
    
    while not queue.empty():
        parent = queue.get()
        visited.add(parent)
        children = set(graph.neighbors(parent)) - visited
        for child in children:
            d.add_edge(parent, child, graph[parent][child])
            queue.put(child)
            
    return d

def entrylog(log, functionName, arguments=None):
    if arguments == None:
        log.debug("Entering function %s", functionName)
    else:
        log.debug("Entering function %s with arguments: %s", functionName, arguments)

def exitlog(log, functionName, returnValue=None):
    if returnValue == None:
        log.debug("Exiting function %s", functionName)
    else:
        log.debug("Exiting function %s with return value: %s", functionName, returnValue)

def is_os_64bit():
        return platform.machine().endswith('64')
    
def getThreadId():
    if platform.system() == 'Linux':
        if platform.architecture()[0] == '64bit':
            return ctypes.CDLL('libc.so.6').syscall(186)
        else:
            return ctypes.CDLL('libc.so.6').syscall(224)
        
    return -1

def toControlPlaneNodeName(nodename):
    if nodename not in ['localhost', '127.0.0.1'] and '.' not in nodename:
        nodename += '.%s.%s' % (testbed.getExperiment(), testbed.getProject())
    return nodename

def loadIDL(agentName, expProcdureFile):
    expAAL = loadYaml(expProcdureFile)
    tar = tarfile.open(expAAL['agents'][agentName]['path'])
    for member in tar.getmembers():
        if member.name.endswith('.idl'):
            f = tar.extractfile(member)
            content = f.read()
            config = yaml.load(content)
            return config

def getNodesFromAAL(filename):
    nodeSet = set()
    if filename:
        aaldata = yaml.load(open(filename, 'r')) 
        for nodes in aaldata['groups'].values():
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
