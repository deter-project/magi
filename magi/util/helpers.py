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

def writeYaml(data, filename):
    """ Load the configuration data from file """
    fp = open(filename, 'w')
    stream = yaml.dump(data, fp)
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

def getNodesFromAAL(filenames):
    nodeSet = set()
    if filenames:
        for filename in toSet(filenames):
            aaldata = yaml.load(open(filename, 'r')) 
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
    
    return "%s.%s.%s" % (dbdl['configHost'], expdl['experimentName'], expdl['projectName'])

def getExperimentNodeList(experimentConfigFile=None, project=None, experiment=None):
    if not experimentConfigFile:
        if not project or not experiment:
            raise RuntimeError('Either the experiment config file or both project and experiment name needs to be provided')
        experimentConfigFile = getExperimentConfigFile(project, experiment)
        
    expdl = yaml.load(open(experimentConfigFile, 'r'))['expdl']
    
    return expdl['nodeList']
    
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

#def getAllExperimentNodes(project, experiment):
#    cmd = "/usr/testbed/bin/node_list -e %s,%s -c" % (project, experiment)
#    (output, err) = Popen(cmd.split(), stdout=PIPE).communicate()
#    nodeset = set()
#    if output:
#        for node in output.split('\n')[0].split(' '):
#            if node and not node.startswith('tbdelay'):
#                nodeset.add(node)
#    return nodeset
