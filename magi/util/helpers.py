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

TRANSPORT_TCP = 'TCP'
TRANSPORT_MULTICAST = 'Multicast'

ALL = '__ALL__'
DEFAULT = '__DEFAULT__'

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
    """ Load YAML-formatted data from file """
    fp = open(filename, 'r')
    if hasattr (yaml, 'CLoader'):
        Loader = yaml.CLoader
    else:
        Loader = yaml.Loader
    data = yaml.load(fp, Loader=Loader)
    fp.close()
    return data

def writeYaml(data, filename):
    """ Write YAML-formatted data to file """
    fp = open(filename, 'w')
    if hasattr (yaml, 'CDumper'):
        Dumper = yaml.CDumper
    else:
        Dumper = yaml.Dumper
    stream = yaml.dump(data, fp, Dumper=Dumper)
    fp.close()
    return stream

def readPropertiesFile(filename):
    import ConfigParser
    import io
    parser = ConfigParser.RawConfigParser()
    properties = '[root]\n' + open(filename, 'r').read()
    parser.readfp(io.BytesIO(properties))
    kv_pairs = parser.items('root')
    return dict(kv_pairs)

def toSet(value):
    if isinstance(value, set):
        return value
    if isinstance(value, list):
        return set(value)
    if isinstance(value, str):    
        return set([s.strip() for s in value.split(',')])
    if value is None:
        return set()
    return set([value])

def getFQCN(object):
    return object.__module__ + "." + object.__class__.__name__
    
def createClassInstance(fqcn):
    import importlib
    moduleName = '.'.join(fqcn.split('.')[:-1])
    className = fqcn.split(".")[-1]
    module_ = importlib.import_module(moduleName)
    class_ = getattr(module_, className)
    instance = class_()
    return instance
    
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

def entrylog(log, functionName, arguments=None, level=logging.DEBUG):
    if arguments == None:
        log.log(level, "Entering function %s", functionName)
    else:
        log.log(level, "Entering function %s with arguments: %s", functionName, arguments)

def exitlog(log, functionName, returnValue=None, level=logging.DEBUG):
    if returnValue == None:
        log.log(level, "Exiting function %s", functionName)
    else:
        log.log(level, "Exiting function %s with return value: %s", functionName, returnValue)

def is_os_64bit():
        return platform.machine().endswith('64')
    
def getThreadId():
    if platform.system() == 'Linux':
        if platform.architecture()[0] == '64bit':
            return ctypes.CDLL('libc.so.6').syscall(186)
        else:
            return ctypes.CDLL('libc.so.6').syscall(224)
        
    return -1

def createSSHTunnel(server, lport, rhost, rport, username=None):
    """
        Create a SSH tunnel and wait for it to be setup before returning.
        Return the SSH command that can be used to terminate the connection.
    """
    if username:
        server = "%s@%s" %(username, server)
    #TODO: Find a better way to create SSH tunnel
    #In order to find out if the ssh process is setup correctly, it needs to 
    #be sent to background (-f). This along with ExitOnForwardFailure makes 
    #the client wait for forwarding to be successfully established before 
    #placing itself in the background. 
    #One issue with this approach is that we do not have a clean way to get to 
    #the process id of the background process.
    #However, if the ssh process is started in foreground, there isn't a way 
    #to figure out if the forwarding is setup successfully or not. This is 
    #because in case of a successful connection, there will be no output 
    #until the process terminates.
    ssh_cmd = "ssh %s -L %d:%s:%d -f -o ExitOnForwardFailure=yes -N" % (server, lport, rhost, rport)
    tun_proc = Popen(ssh_cmd.split(), stderr=PIPE)
    p = tun_proc.wait()
    if p != 0:
        raise RuntimeError, 'Error creating tunnel: %s :: %s' %(str(p), tun_proc.communicate()[1])
    return ssh_cmd

def terminateProcess(cmd):
    os.system("kill -9 `ps -ef | grep '" + cmd + "' | grep -v grep | awk '{print $2}'`")
    
def toControlPlaneNodeName(nodename):
    from magi.testbed import testbed
    return testbed.toControlPlaneNodeName(nodename)

def loadIDL(agentName, expProcdureFile):
    expAAL = loadYaml(expProcdureFile)
    tar = tarfile.open(expAAL['agents'][agentName]['path'])
    for member in tar.getmembers():
        if member.name.endswith('.idl'):
            f = tar.extractfile(member)
            config = loadYaml(f)
            return config

def getNodesFromAAL(filenames):
    nodeSet = set()
    if filenames:
        for filename in toSet(filenames):
            aaldata = loadYaml(filename)
            for nodes in aaldata['groups'].values():
                nodeSet.update(nodes)
    return nodeSet

def getExperimentConfigFile(project, experiment):
    configFile = os.path.join('/proj', project, 'exp', experiment, 'experiment.conf')
    return configFile

def getBridge(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
    
    mesdl = loadYaml(experimentConfigFile)['mesdl']
    bridges = mesdl['bridges']
    
    return (bridges[0]['server'], bridges[0]['port'])

def getExperimentDBHost(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
    
    dbdl = loadYaml(experimentConfigFile)['dbdl']
    
    isDBSharded = dbdl['isDBSharded']
    
    if isDBSharded:
        return (toControlPlaneNodeName(dbdl['globalServerHost']), dbdl['globalServerPort'])
    else:
        sensorToCollectorMap = dbdl['sensorToCollectorMap']
        return (toControlPlaneNodeName(dbdl['sensorToCollectorMap'][DEFAULT]), dbdl['collectorPort'])
    
def getExperimentNodeList(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
        
    expdl = loadYaml(experimentConfigFile)['expdl']
    
    return expdl['nodeList']
    
def getMagiNodeList(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
        
    expdl = loadYaml(experimentConfigFile)['expdl']
    return expdl['magiNodeList']    

def getServer(nodeList):
    # returns control if  it finds a node named "control" 
    # in the given node list otherwise it returns the
    # first node in the alpha-numerically sorted list
    if isinstance(nodeList, set):
        nodeList = list(nodeList)
    if not isinstance(nodeList, list):
        raise TypeError("node list should be a set or list")
    nodeList.sort()
    host = nodeList[0]
    for node in nodeList:
        if 'control' == node.lower():
            host = 'control'
            break
    return host 

def printDBfields(agentidl):
            agentname = agentidl.get('display', 'Agent')
            desc = agentidl.get('description', 'No description Available')
            print
            print 'Agent Name:', agentname
            print desc
            print
            print 'Format of display: Fieldname(type/value): Description'
            print
            print 'created (float/sec.msec): Timestamp logged by sensor'
            print 'host (str): Hostname where sensor is deployed'
            print 'agent (str):  Name of sensor'
            dbitems = agentidl.get('dbfields',{})
            for field,desc in dbitems.items():
                print field,'(',desc['keytype'],'/',desc['value'],')',':',desc['keydesc']
            print

chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
def getMulticast(arg1, arg2, channel):
    return "239.255.%d.%d" % (_str2byte(arg1), (_str2byte(arg2)+channel)%255)
def _intval(x, y):
    return x + chars.find(y)
def _str2byte(strin):
    return reduce(_intval, strin, 0) % 255

#def getAllExperimentNodes(project, experiment):
#    cmd = "/usr/testbed/bin/node_list -e %s,%s -c" % (project, experiment)
#    (output, err) = Popen(cmd.split(), stdout=PIPE).communicate()
#    nodeset = set()
#    if output:
#        for node in output.split('\n')[0].split(' '):
#            if node and not node.startswith('tbdelay'):
#                nodeset.add(node)
#    return nodeset
