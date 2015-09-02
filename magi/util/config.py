
"""
Example experiment configuration file 

dbdl:
  sensorToCollectorMap: {node1: node1, node2: node2}
  configHost: node1
  isDBEnabled: true

expdl:
  distributionPath: /share/magi/current
  projectName: myProject
  experimentName: myExperiment
  nodePaths: {config: /var/log/magi, db: /var/lib/mongodb, logs: /var/log/magi, temp: /tmp}
  testbedPaths: {experimentDir: /proj/myProject/exp/myExperiment}
  nodeList: [node1, node2]
  aal: /proj/myProject/exp/myExperiment/procedure.aal

mesdl:
  bridges:
  - {port: 18808, server: node1.myExperiment.myProject, type: TCPServer}
  - {port: 28808, server: node1.myExperiment.myProject, type: TCPServer}
  overlay:
  - members: [__ALL__]
    port: 28808
    server: node1.myExperiment.myProject
    type: TCPTransport

"""

"""
  Example node configuration file:

localInfo:
  nodename: node1
  hostname: node1.myExperiment.myProject.isi.deterlab.net
  controlif: eth1
  controlip: 192.168.1.31
  distribution: Ubuntu 12.04 (precise)
  architecture: 64bit
  configDir: /var/log/magi
  logDir: /var/log/magi
  dbDir: /var/lib/mongodb
  tempDir: /tmp
  interfaceInfo:
    eth3:
      expif: eth3
      expip: 10.1.1.2
      expmac: 00151757c7c2
      linkname: link
      peernodes: [node2]

transports:
   - { class: MulticastTransport, address: 239.255.1.2, localaddr: 192.168.1.1, port: 18808 }
   - { class: TCPTransport, address: 192.168.2.1, port: 18808 }
   - { class: TCPServer, address: 0.0.0.0, port: 18808 }
   - { class: SSLTransport, address: 172.16.1.1, port: 18810, cafile: /proj/P/exp/E/tbdata/ca.pem, nodefile: /proj/P/exp/E/tbdata/node.pem, matchingOU: P.E }
   - { class: SSLServer, address: 0.0.0.0, port: 18810, cafile: /proj/P/exp/E/tbdata/ca.pem, nodefile: /proj/P/exp/E/tbdata/node.pem, matchingOU: P.E }

database:
  isDBEnabled: true
  sensorToCollectorMap: {node1: node1, node2: node2}
  configHost: node1
  
software:
- {dir: /share/magi/current/Linux-Ubuntu12.04-x86_64, type: rpmfile}
- {dir: /share/magi/current/Linux-Ubuntu12.04-x86_64, type: archive}
- {type: apt}
- {dir: /share/magi/current/source, type: source}
- {dir: /tmp/src, type: source}

"""

import copy
import logging
import os
import platform
from socket import gethostbyname, gaierror
import sys

from magi.db.Server import DATABASE_SERVER_PORT, ROUTER_SERVER_PORT
from magi.testbed import testbed
from networkx.readwrite import json_graph

import helpers


DEFAULT_DIST_DIR  = "/share/magi/current/"
DEFAULT_DB_ENABLED = True
DEFAULT_DB_SHARDED = False
DEFAULT_TEMP_DIR  = "/tmp"
DEFAULT_TRANSPORT_CLASS = helpers.TRANSPORT_MULTICAST

NODE_DIR = "/var/log/magi"
EXPERIMENT_DIR = testbed.getExperimentDir()

NODE_CONFIG = dict()
EXP_CONFIG = dict()

log = logging.getLogger(__name__)

def setNodeDir(nodeDir):
    global NODE_DIR
    NODE_DIR = nodeDir
    
def setExperimentDir(experimentDir):
    global EXPERIMENT_DIR
    EXPERIMENT_DIR = experimentDir
     
def getExperimentConfig():
    """ Fetch the experiment-wide configuration"""
    global EXP_CONFIG
    if not EXP_CONFIG:
        EXP_CONFIG = loadExperimentConfig(experimentConfig=
                                          getExperimentConfFile())
    return EXP_CONFIG

def getNodeConfig():
    """ Fetch node specific configuration """
    global NODE_CONFIG
    if not NODE_CONFIG:
        NODE_CONFIG = loadNodeConfig(nodeConfig=getNodeConfFile(),
                                     experimentConfig=getExperimentConfig())
    return NODE_CONFIG

def getConfig():
    """ Fetch the node configuration"""
    return getNodeConfig()

def getNodeName():
    return getNodeConfig()['localInfo']['nodename']

def getDbConfigHost():
    return getNodeConfig()['database'].get('configHost')

def getConfigDir():
    try:
        return NODE_CONFIG['localInfo']['configDir']
    except:
        return os.path.join(NODE_DIR, 'config')
    
def getNodeConfFile():
    return os.path.join(getConfigDir(), 'node.conf')

def getExperimentConfFile():
    return os.path.join(getConfigDir(), 'experiment.conf')

def getMagiPidFile():
    return os.path.join(getConfigDir(), 'magi.pid')

def getLogDir():
    try:
        return NODE_CONFIG['localInfo']['logDir']
    except:
        return os.path.join(NODE_DIR, 'logs')

def getDbDir():
    try:
        return NODE_CONFIG['localInfo']['dbDir']
    except:
        return os.path.join(NODE_DIR, 'db')

def getTempDir():
    try:
        return NODE_CONFIG['localInfo']['tempDir']
    except:
        return DEFAULT_TEMP_DIR

def getDistDir():
    try:
        return NODE_CONFIG['localInfo']['distributionPath']
    except:
        return DEFAULT_DIST_DIR
    
def getExperimentDir():
    return getNodeConfig()['localInfo']['experimentDir']

def getTopoGraph():
    return json_graph.node_link_graph(getNodeConfig()['topoGraph'])

def getMagiNodes():
    try:
        return EXP_CONFIG['expdl']['magiNodeList']
    except:
        return testbed.getTopoGraph().nodes()

## EXPERIMENT CONFIGURATION ##

def createExperimentConfig(distributionPath=DEFAULT_DIST_DIR, 
                           isDBEnabled=DEFAULT_DB_ENABLED,
                           isDBSharded=DEFAULT_DB_SHARDED,
                           transportClass=DEFAULT_TRANSPORT_CLASS):
    log.info("Creating default experiment configuration") 
    return loadExperimentConfig(distributionPath=distributionPath, 
                                isDBEnabled=isDBEnabled,
                                isDBSharded=isDBSharded,
                                transportClass=transportClass)

def loadExperimentConfig(experimentConfig={}, distributionPath=None, 
                         isDBEnabled=None, isDBSharded=None, 
                         transportClass=DEFAULT_TRANSPORT_CLASS):
    """ Load the experiment-wide configuration data from file, filename can be overriden """
    # Do not modify the input object
    experimentConfig = copy.deepcopy(experimentConfig)
    
    try:
        if (type(experimentConfig) == str):
            log.info("Loading given experiment configuration from %s" %(experimentConfig))
            experimentConfig = helpers.loadYaml(experimentConfig)
    except:
        log.error("Error loading given experiment configuration file. Loading default.")
        experimentConfig = dict()
        
    experimentConfig = validateExperimentConfig(experimentConfig=experimentConfig, 
                                                distributionPath=distributionPath, 
                                                isDBEnabled=isDBEnabled,
                                                isDBSharded=isDBSharded,
                                                transportClass=transportClass)
    
    #setting global experiment configuration variable
    global EXP_CONFIG
    EXP_CONFIG = experimentConfig
    
    return experimentConfig

def validateExperimentConfig(experimentConfig={}, 
                             distributionPath=None, 
                             isDBEnabled=None,
                             isDBSharded=None, 
                             transportClass=DEFAULT_TRANSPORT_CLASS):
    """ 
        Validate an experiment wide configuration 
    """
    # Do not modify the input object
    experimentConfig = copy.deepcopy(experimentConfig)
    
    if not experimentConfig:
        experimentConfig = dict()
    
    if not isinstance(experimentConfig, dict):
        raise TypeError("Experiment configuration should be a dictionary")
        
    expdl = validateExpDL(expdl=experimentConfig.get('expdl'), 
                          distributionPath=distributionPath)
    mesdl = validateMesDL(mesdl=experimentConfig.get('mesdl'), 
                          magiNodeList=expdl['magiNodeList'],
                          transportClass=transportClass)
    dbdl = validateDBDL(dbdl=experimentConfig.get('dbdl'), 
                        isDBEnabled=isDBEnabled, 
                        isDBSharded=isDBSharded,
                        magiNodeList=expdl['magiNodeList'])
    
    experimentConfig['expdl'] = expdl
    experimentConfig['mesdl'] = mesdl
    experimentConfig['dbdl'] = dbdl
    
    return experimentConfig

def getDefaultMESDL():
    """ Create a default mesdl for the control plane """
    log.info("Creating default mesdl") 
    return validateMesDL()

def validateMesDL(mesdl={}, magiNodeList=getMagiNodes(), 
                  transportClass=DEFAULT_TRANSPORT_CLASS):
    """ Validate messaging description """
    # Do not modify the input object
    mesdl = copy.deepcopy(mesdl)
    
    if not mesdl:
        mesdl = dict()
        
    if not isinstance(mesdl, dict):
        raise TypeError("MesDL should be a dictionary")
    
    server = helpers.toControlPlaneNodeName(getServer(magiNodeList))
    
    if not mesdl:
        log.info("Using %s as server node", server) 
        mesdl['bridges'] = list()
        mesdl['overlay'] = list()
        mesdl['bridges'].append({'type': 'TCPServer', 
                                 'server': server, 'port': 18808})
        if transportClass == helpers.TRANSPORT_TCP:
            mesdl['bridges'].append({'type': 'TCPServer', 
                                     'server': server, 'port': 28808})
            mesdl['overlay'].append({'type': 'TCPTransport', 
                                     'members': ['__ALL__'], 
                                     'server': server, 'port': 28808})
        elif transportClass == helpers.TRANSPORT_MULTICAST:
            mesdl['overlay'].append({'type': 'MulticastTransport', 
                                     'members': ['__ALL__'], 
                                     'address': testbed.getMulticastAddress(), 
                                     'port': 28808})
        else:
            raise TypeError("Invalid transport type. Should be one of %s" 
                        %[helpers.TRANSPORT_TCP, helpers.TRANSPORT_MULTICAST])
    else:
        bridges = mesdl.get('bridges', {})
        if not bridges:
            log.error('At least one bridge is required for external connections')
            bridges.append({'type': 'TCPServer', 
                            'server': server, 'port': 18808})
            
    return mesdl

def getDefaultDBDL(isDBEnabled=DEFAULT_DB_ENABLED):
    """ Create a default database description """
    log.info("Creating default dbdl")
    return validateDBDL(dbdl=dbdl, isDBEnabled=isDBEnabled)

def validateDBDL(dbdl={}, isDBEnabled=None, 
                 isDBSharded=None, magiNodeList=getMagiNodes()):
    """ Validate database description """
    # Do not modify the input object
    dbdl = copy.deepcopy(dbdl)
    
    if not dbdl:
        dbdl = dict()
    
    if not isinstance(dbdl, dict):
        raise TypeError("DBDL should be a dictionary")
    
    if isDBEnabled is not None:
        dbdl['isDBEnabled'] = isDBEnabled
    else:
        isDBEnabled = dbdl.setdefault('isDBEnabled', DEFAULT_DB_ENABLED)
        
    if isDBEnabled:
        if isDBSharded is None:
            isDBSharded = dbdl.get('isDBSharded', DEFAULT_DB_SHARDED)
            
        sensorToCollectorMap = validateSensorToColletorMap(
                        sensorToCollectorMap=dbdl.get('sensorToCollectorMap'), 
                        magiNodes=magiNodeList,
                        isDBSharded=isDBSharded)
        
        # if sensorToCollectorMap is empty
        if not sensorToCollectorMap:
            dbdl = {}
            dbdl['isDBEnabled'] = False
            return dbdl
            
        dbdl['sensorToCollectorMap'] = sensorToCollectorMap
        
        collectors = sensorToCollectorMap.values()
        
        # No need to setup a sharded setup in case of just one collector
        if len(collectors) == 1:
            isDBSharded = False
            
        dbdl['isDBSharded'] = isDBSharded
        
        if dbdl.get('configHost') not in collectors:
            dbdl['configHost'] = getServer(collectors)
        dbdl['configHost'] = helpers.toControlPlaneNodeName(dbdl['configHost'])
            
        if isDBSharded:
            dbdl['configPort'] = ROUTER_SERVER_PORT
        else:
            dbdl['configPort'] = DATABASE_SERVER_PORT
    else:
        dbdl = {}
        dbdl['isDBEnabled'] = False
        
    return dbdl

def validateSensorToColletorMap(sensorToCollectorMap={}, 
                                magiNodes=getMagiNodes(), 
                                isDBSharded=DEFAULT_DB_SHARDED):
    """ Validate sensor to collector mappings """
    # Do not modify the input object
    sensorToCollectorMap = copy.deepcopy(sensorToCollectorMap)
    
    if not sensorToCollectorMap:
        sensorToCollectorMap = dict()
    
    if not isinstance(sensorToCollectorMap, dict):
        raise TypeError("sensorToCollectorMap should be a dictionary")
    
    if not sensorToCollectorMap:
        if isDBSharded:
            sensorToCollectorMap = {nodeName:nodeName for nodeName in magiNodes}
        else:
            sensorToCollectorMap = {helpers.ALL : getServer(magiNodes)}
    else:
        # Cleaning up existing sensorToCollectorMap
        # Removing non-existing experiment nodes
        for (sensor, collector) in sensorToCollectorMap.copy().iteritems():
            if sensor not in magiNodes + [helpers.ALL]:
                del sensorToCollectorMap[sensor]
            elif collector not in magiNodes:
                # Invalid default collector
                if sensor == helpers.ALL:
                    del sensorToCollectorMap[sensor]
                else:
                    sensorToCollectorMap[sensor] = sensor
        
        # Validate that all nodes have a valid collector
        if helpers.ALL not in sensorToCollectorMap:
            for node in magiNodes:
                if node not in sensorToCollectorMap:
                    sensorToCollectorMap[node] = node
    
    return sensorToCollectorMap

def getDefaultExpDL(distributionPath=DEFAULT_DIST_DIR):
    """ Create a default experiment description """
    log.info("Creating default expdl") 
    return validateExpDL(distributionPath=distributionPath)

def validateExpDL(expdl={}, distributionPath=None):
    """ """
    # Do not modify the input object
    expdl = copy.deepcopy(expdl)
    
    if not expdl:
        expdl = dict()
        
    if not isinstance(expdl, dict):
        raise TypeError("ExpDL should be a dictionary")
    
    expdl.setdefault('topoGraph', json_graph.node_link_data(testbed.getTopoGraph()))
    nodeList = json_graph.node_link_graph(expdl['topoGraph']).nodes()
    nodeList.sort()
    expdl['nodeList'] = nodeList
    expdl.setdefault('magiNodeList', nodeList)
    if distributionPath:
        expdl['distributionPath'] = distributionPath
    else:
        expdl.setdefault('distributionPath', DEFAULT_DIST_DIR)
    
    nodePaths = expdl.setdefault('nodePaths', dict())
    
    nodeDir = nodePaths.setdefault('root', NODE_DIR)
    nodePaths.setdefault('config', os.path.join(nodeDir, 'config'))
    nodePaths.setdefault('logs', os.path.join(nodeDir, 'logs'))
    nodePaths.setdefault('db', os.path.join(nodeDir, 'db'))
    nodePaths.setdefault('temp', DEFAULT_TEMP_DIR)
    
    testbedPaths = expdl.setdefault('testbedPaths', dict())
    testbedPaths['experimentDir'] = testbed.getExperimentDir()
    
    expdl.setdefault('aal', os.path.join(testbed.getExperimentDir(), "procedure.aal"))
    
    return expdl

## NODE CONFIGURATION ##

def createNodeConfig(experimentConfig={}):
    log.info("Creating default node configuration") 
    return loadNodeConfig(experimentConfig=experimentConfig)

def loadNodeConfig(nodeConfig={}, experimentConfig=None):
    # Do not modify the input object
    nodeConfig = copy.deepcopy(nodeConfig)
    experimentConfig = copy.deepcopy(experimentConfig)
    
    try:
        if (type(nodeConfig) == str):
            log.info("Loading given node configuration from %s" %(nodeConfig))
            nodeConfig = helpers.loadYaml(nodeConfig)
    except:
        log.error("Error loading given node configuration file. Loading default.")
        nodeConfig = dict()
        
    if experimentConfig == None:
        experimentConfig = getExperimentConfig()
    else:        
        try:
            if (type(experimentConfig) == str):
                log.info("Loading given experiment configuration from %s" %(experimentConfig))
                experimentConfig = helpers.loadYaml(experimentConfig)
        except:
            log.info("Error loading given experiment configuration file. Creating default.")
            experimentConfig = dict()
        
    nodeConfig = validateNodeConfig(nodeConfig=nodeConfig, experimentConfig=experimentConfig)
    
    #setting global node configuration variable
    global NODE_CONFIG
    NODE_CONFIG = nodeConfig
    
    return nodeConfig

def validateNodeConfig(nodeConfig={}, experimentConfig={}):
    """
        Validate a node specific configuration, based on experiment-wide configuration
    """
    # Do not modify the input object
    nodeConfig = copy.deepcopy(nodeConfig)
    
    if not nodeConfig:
        nodeConfig = dict()
        
    if not isinstance(nodeConfig, dict):
        raise TypeError("Node configuration should be a dictionary")
        
    # Do not modify the input object
    experimentConfig = copy.deepcopy(experimentConfig)
    experimentConfig = validateExperimentConfig(experimentConfig=experimentConfig)
    
    # Information about the local node for reference 
    localInfo = nodeConfig.setdefault('localInfo', {})
    
    localInfo.setdefault('nodename', testbed.nodename)
    localInfo['hostname'] = platform.uname()[1]
    localInfo['distribution'] = "%s %s (%s)" %(platform.dist()[0], platform.dist()[1], platform.dist()[2])
    localInfo['architecture'] = platform.architecture()[0]
    localInfo.setdefault('controlip', testbed.controlip)
    localInfo.setdefault('controlif', testbed.controlif)
    
    #TODO: Setting testbed.nodename for desktop mode
    # testbed.getTopoGraph() uses the nodename
    import magi.testbed
    if isinstance(testbed, magi.testbed.desktop.DesktopExperiment):
        testbed.setNodeName(localInfo['nodename'])
    
    expdl = experimentConfig['expdl']
    expNodePaths = expdl['nodePaths']
    localInfo.setdefault('rootDir', expNodePaths['root'])
    localInfo.setdefault('configDir', expNodePaths['config'])
    localInfo.setdefault('logDir', expNodePaths['logs'])
    localInfo.setdefault('dbDir', expNodePaths['db'])
    localInfo.setdefault('tempDir', expNodePaths['temp'])
    localInfo.setdefault('distributionPath', expdl['distributionPath'])
    localInfo.setdefault('processAgentsCommPort', 18809)
    
    testbedPaths = expdl['testbedPaths']
    localInfo.setdefault('experimentDir', testbedPaths['experimentDir'])
    
    interfaceInfo = dict()
    topoGraph = testbed.getTopoGraph()
    for ip in testbed.getLocalIPList():
        linkname = 'unknown'
        peerNodes = []
        for linkInfo in topoGraph.node[testbed.nodename]['links'].values():
            if linkInfo['ip'] == ip:
                linkname = linkInfo['name']
                peerNodes = linkInfo['peerNodes']
                break
        interfaceInfo[testbed.getInterfaceInfo(ip).name] = {'expip': ip, 
                                                            'expif': testbed.getInterfaceInfo(ip).name, 
                                                            'expmac': testbed.getInterfaceInfo(ip).mac, 
                                                            'linkname': linkname, 
                                                            'peernodes': peerNodes}
    
    localInfo['interfaceInfo'] = interfaceInfo
    
    nodeConfig.setdefault('topoGraph', expdl['topoGraph'])
    
    # Information about the location of the software libraries 
    softwareConfig = nodeConfig.setdefault('software', [])
    
    if not softwareConfig:
        # Try our local prebuilt software first 
        # this gets around dist installers pointing outside the testbed and long timeouts
        rootdir = expdl.get('distributionPath', sys.path[0])
        softwareConfig.append({'type': 'rpmfile', 'dir': os.path.join(rootdir, getArch())})
        softwareConfig.append({'type': 'archive', 'dir': os.path.join(rootdir, getArch())})
    
        # then dist installer
        osname = platform.uname()[0].lower()
        dist = platform.dist()[0].lower()
        if dist in ('ubuntu', 'debian'):
            softwareConfig.append({ 'type': 'apt'})
        elif dist in ('redhat', 'fedora'):
            softwareConfig.append({ 'type': 'yum'})
        elif osname in ('freebsd', ):
            softwareConfig.append({ 'type': 'pkgadd'})
    
        # then build as last resort
        softwareConfig.append({'type': 'source',  'dir': os.path.join(rootdir, 'source')})
        softwareConfig.append({'type': 'source',  'dir': os.path.join('/tmp/src')})

        
    # Infomation about the transports  
    # Read the messaging overlay description for the experiment and create 
    # the required transports for this node
    transportsConfig = nodeConfig.setdefault('transports', [])
     
    mesdl = experimentConfig['mesdl']
    log.debug("Mesdl from experiment wide configuration: %s", mesdl)
    
    if not transportsConfig:
        nodename = helpers.toControlPlaneNodeName(localInfo['nodename'])
        # For each external connection, add a TCPServer transport    
        for bridge in mesdl['bridges']:
            log.debug("Bridge: %s", bridge)
            if nodename == bridge['server']:
                # A transport server is added on the node  
                # This is used to provide an external facing connection to the magi messaging network on port extport (typically 18808)  
                transportsConfig.append({ 'class': bridge['type'], 'address': '0.0.0.0', 'port': bridge['port']})
        
        # For each messaging overlay that the local node is part of, 
        # Add an apporpriate transport 
        # NOTE: We are just adding TCPTransports currently 
        for t in mesdl['overlay']:
            log.debug("Control Plane Overlay: %s", t)
            if t['type'] == 'TCPTransport' and nodename != t['server'] and (nodename in t['members'] or '__ALL__' in t['members']):
                server_name = t['server']
                # DETER/emulab DNS will resolves FQDNs to the control network address, 
                # A FQDN or IP address would be required for connecting with the external world 
                try:
                    server_addr=gethostbyname(server_name)
                except gaierror:
                    log.critical('Using MeSDL file %s\n Unable to resolve node name %s, EXITING', mesdl, server_name)
                    sys.exit(2)
                        
                transportsConfig.append({ 'class': 'TCPTransport', 'address': server_addr, 'port': t['port'] })
                
            elif t['type'] == 'MulticastTransport' and (nodename in t['members'] or '__ALL__' in t['members']):
                transportsConfig.append({ 'class': 'MulticastTransport', 'address': t['address'], 'localaddr': testbed.controlip, 'port': t['port'] })


    #Database Configuration
    databaseConfig = nodeConfig.setdefault('database', {})
    
    dbdl = experimentConfig['dbdl']
    log.debug("Dbdl from experiment wide configuration: %s", dbdl)
    
    if localInfo['nodename'] not in expdl['magiNodeList']:
        databaseConfig['isDBEnabled'] = False
    isDBEnabled = databaseConfig.setdefault('isDBEnabled', dbdl.get('isDBEnabled'))
    if isDBEnabled:
        databaseConfig.setdefault('isDBSharded', dbdl.get('isDBSharded'))
        databaseConfig.setdefault('sensorToCollectorMap', dbdl['sensorToCollectorMap'])
        sensorToCollectorMap = databaseConfig['sensorToCollectorMap']
        validateSensorToColletorMap(sensorToCollectorMap, 
                                    expdl['magiNodeList'])
        databaseConfig.setdefault('configHost', dbdl['configHost'])
        databaseConfig.setdefault('configPort', dbdl['configPort'])
        
    log.debug("Node Configuration: %s", nodeConfig)
    return nodeConfig

def getArch():
    """ Try and get something that is unique to this build environment for tagging built source """
    (name, node, ver, extra, machine, proc) = platform.uname()
    (ostype, release, nick) = platform.dist()
    if ostype != "":
        ver = "%s%s" % (ostype, release)
    if name.startswith('CYGWIN'):
        ver = 'xp'
    arch = "%s-%s-%s" % (name, ver, machine)  # arch string to use for cached software lookup/saving
    return arch.replace('/', '')

def getServer(nodeList):
    # returns control if  it finds a node named "control" 
    # in the given node list otherwise it returns the
    # first node in the alpha-numerically sorted list
    if not isinstance(nodeList, list):
        raise TypeError("node list should be a list")
    nodeList.sort()
    host = nodeList[0]
    for node in nodeList:
        if 'control' == node.lower():
            host = 'control'
            break
    return host    
        
#def keysExist(project=None, experiment=None, keydir=None):
#    """
#        Simple check to see if the specific keys exist before creating them
#    """
#    project = project or testbed.project
#    experiment = experiment or testbed.experiment
#    if keydir is None:
#        keydir = DEFAULT_KEYDIR
#    cafile = os.path.join(keydir, 'ca.pem')
#    nodefile = os.path.join(keydir, 'node.pem')
#    subprocess.call("ls -l %s > /dev/null" % (keydir), shell=True)  # ugly mans way of flushing NFS read cache
#    if not os.path.exists(cafile) or not os.path.exists(nodefile):
#        return False
#    return True
#
#def generateKeys(project=None, experiment=None, keydir=None):
#    """
#        Generate the ca.pem and node.pem files for the specified project/experiment. If
#        keydir is specified, they are placed there.  If not, they are placed in
#        /proj/P/exp/E/tbdata/
#    """
#    project = project or testbed.project
#    experiment = experiment or testbed.experiment
#    if keydir is None:
#        keydir = DEFAULT_KEYDIR
#    cafile = os.path.join(keydir, 'ca.pem')
#    nodefile = os.path.join(keydir, 'node.pem')
#    ou = "%s.%s" % (project, experiment)
#    # find the number of days to the end of 'time' and use that
#    days = (datetime.date.fromtimestamp(0x7fffffff) - datetime.date.today()).days - 10
#
#    run("echo \"03\" > ca.serial", shell=True)
#    run("openssl genrsa -out ca.key 1024", shell=True)
#    run("openssl req -new -x509 -key ca.key -out %s -days %d -subj \"/C=US/ST=CA/O=DETER/OU=%s/CN=CA\"" % (cafile, days, ou), shell=True)
#    run("openssl genrsa -out exp.key 1024", shell=True)
#    run("openssl req -new -key exp.key -out exp.req -subj \"/C=US/ST=CA/O=DETER/OU=%s/CN=node\"" % (ou), shell=True)
#    run("openssl x509 -CA %s -CAkey ca.key -CAserial ca.serial -req -in exp.req -out exp.signed -days %d" % (cafile, days), shell=True)
#    run("openssl pkcs8 -nocrypt -in exp.key -topk8 -outform der -out node.pk8", shell=True)
#    run("cat exp.signed exp.key > %s" % (nodefile), shell=True)
#    run("rm exp.signed exp.key exp.req ca.serial ca.key node.pk8", shell=True)


if __name__ == "__main__":
    print "In main" 
    createExperimentConfig() 
    createNodeConfig()
