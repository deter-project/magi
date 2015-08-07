
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
DEFAULT_DB_SHARDED = True
DEFAULT_TEMP_DIR  = "/tmp"

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

def createExperimentConfig(distributionPath=DEFAULT_DIST_DIR, isDBEnabled=DEFAULT_DB_ENABLED):
    log.info("Creating default experiment configuration") 
    return loadExperimentConfig(distributionPath=distributionPath, isDBEnabled=isDBEnabled)

def loadExperimentConfig(experimentConfig={}, distributionPath=None, isDBEnabled=None):
    """ Load the experiment-wide configuration data from file, filename can be overriden """
    global EXP_CONFIG
    try:
        if (type(experimentConfig) == str):
            log.info("Loading given experiment configuration from %s" %(experimentConfig))
            experimentConfig = helpers.loadYaml(experimentConfig)
    except:
        log.error("Error loading given experiment configuration file. Loading default.")
        experimentConfig = dict()
    EXP_CONFIG = validateExperimentConfig(experimentConfig=experimentConfig, 
                                          distributionPath=distributionPath, 
                                          isDBEnabled=isDBEnabled)
    
#    #write experiment config file
#    expConfFile = getExperimentConfFile()
#    helpers.makeDir(os.path.dirname(expConfFile))
#    fp = open(expConfFile, 'w')
#    fp.write(yaml.safe_dump(experimentConfig))
#    fp.close()
    
    return EXP_CONFIG

def validateExperimentConfig(experimentConfig, distributionPath=None, isDBEnabled=None):
    if not experimentConfig:
        experimentConfig = dict()
        
    expdl = experimentConfig.get('expdl', {})
    mesdl = experimentConfig.get('mesdl', {})
    dbdl = experimentConfig.get('dbdl', {})
    
    experimentConfig['expdl'] = validateExpDL(expdl=expdl, 
                                            distributionPath=distributionPath)
    experimentConfig['mesdl'] = validateMesDL(mesdl=mesdl, 
                        magiNodeList=experimentConfig['expdl']['magiNodeList'])
    experimentConfig['dbdl'] = validateDBDL(dbdl=dbdl, isDBEnabled=isDBEnabled, 
                        magiNodeList=experimentConfig['expdl']['magiNodeList'])

    return experimentConfig
    
def getDefaultMESDL():
    """ Create a default mesdl for the control plane """
    log.info("Creating default mesdl") 
    return validateMesDL()

def validateMesDL(mesdl={}, magiNodeList=testbed.getTopoGraph().nodes()):
    """ Validate messaging description """
    server = helpers.toControlPlaneNodeName(getServer(magiNodeList))
    if not mesdl:
        mesdl = dict()
        log.info("Using %s as server node", server) 
        mesdl['bridges'] = list()
        mesdl['overlay'] = list()
        mesdl['bridges'].append({'type': 'TCPServer', 
                                 'server': server, 'port': 18808})
        transportClass = 'Multicast'
        if transportClass == 'TCP':
            mesdl['bridges'].append({'type': 'TCPServer', 
                                     'server': server, 'port': 28808})
            mesdl['overlay'].append({'type': 'TCPTransport', 
                                     'members': ['__ALL__'], 
                                     'server': server, 'port': 28808})
        elif transportClass == 'Multicast':
            mesdl['overlay'].append({'type': 'MulticastTransport', 
                                     'members': ['__ALL__'], 
                                     'address': testbed.getMulticastAddress(), 
                                     'port': 28808})
    else:
        bridges = mesdl.get('bridges', {})
        if not bridges:
            log.error('At least one bridge is required for external connections')
            bridges.append({'type': 'TCPServer', 
                            'server': server, 'port': 18808})
    return mesdl

def getDefaultDBDL(isDBEnabled=True):
    """ Create a default database description """
    log.info("Creating default dbdl") 
    return validateDBDL(isDBEnabled=isDBEnabled)

def validateDBDL(dbdl={}, isDBEnabled=None, magiNodeList=testbed.getTopoGraph().nodes()):
    """ Validate database description """
    if not dbdl:
        dbdl = dict()
    
    if isDBEnabled is not None:
        dbdl['isDBEnabled'] = isDBEnabled
    else:
        isDBEnabled = dbdl.setdefault('isDBEnabled', DEFAULT_DB_ENABLED)
        
    if isDBEnabled:
        isDBSharded = dbdl.setdefault('isDBSharded', 
                                      False if len(magiNodeList) == 1 
                                      else DEFAULT_DB_SHARDED)
        sensorToCollectorMap = dbdl.setdefault('sensorToCollectorMap', {})
        validateSensorToColletorMap(sensorToCollectorMap, magiNodeList)
        # if sensorToCollectorMap is empty
        if not sensorToCollectorMap:
            dbdl = {}
            dbdl['isDBEnabled'] = False
            return dbdl
            
        collectors = sensorToCollectorMap.values()
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

def validateSensorToColletorMap(sensorToCollectorMap, magiNodes):
    if not isinstance(sensorToCollectorMap, dict):
        sensorToCollectorMap = dict()
        for node in magiNodes:
            sensorToCollectorMap[node] = node
    else:
        # Cleaning up existing sensorToCollectorMap
        # Removing non-existing experiment nodes
        for (sensor, collector) in sensorToCollectorMap.iteritems():
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


def getDefaultExpDL(distributionPath=DEFAULT_DIST_DIR):
    """ Create a default experiment description """
    log.info("Creating default expdl") 
    return validateExpDL(distributionPath=distributionPath)
    
def validateExpDL(expdl={}, distributionPath=None):
    """ """
    if not expdl:
        expdl = dict()
        
    expdl.setdefault('topoGraph', json_graph.node_link_data(testbed.getTopoGraph()))
    nodeList = testbed.getTopoGraph().nodes()
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
    global NODE_CONFIG
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
        
    NODE_CONFIG = validateNodeConfig(nodeConfig=nodeConfig, experimentConfig=experimentConfig)
    
#    #write experiment config file
#    expConfFile = getExperimentConfFile()
#    helpers.makeDir(os.path.dirname(expConfFile))
#    fp = open(expConfFile, 'w')
#    fp.write(yaml.safe_dump(experimentConfig))
#    fp.close()
#    
#    #write node configuration to file
#    nodeConfFile = getNodeConfFile()
#    helpers.makeDir(os.path.dirname(nodeConfFile))
#    fp = open(nodeConfFile, 'w')
#    fp.write(yaml.safe_dump(NODE_CONFIG))
#    fp.close()

    return NODE_CONFIG

def validateNodeConfig(nodeConfig, experimentConfig={}):
    """
        Validate a node specific configuration, based on experiment-wide configuration
    """
    if not nodeConfig:
        nodeConfig = dict()
        
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
    
    expNodePaths = experimentConfig['expdl']['nodePaths']
    localInfo.setdefault('rootDir', expNodePaths['root'])
    localInfo.setdefault('configDir', expNodePaths['config'])
    localInfo.setdefault('logDir', expNodePaths['logs'])
    localInfo.setdefault('dbDir', expNodePaths['db'])
    localInfo.setdefault('tempDir', expNodePaths['temp'])
    localInfo.setdefault('distributionPath', experimentConfig['expdl']['distributionPath'])
    
    testbedPaths = experimentConfig['expdl']['testbedPaths']
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
    #config['processAgentsCommPort'] = None
    
    nodeConfig.setdefault('topoGraph', experimentConfig['expdl']['topoGraph'])
    
    # Information about the location of the software libraries 
    softwareConfig = nodeConfig.setdefault('software', [])
    
    if not softwareConfig:
        osname = platform.uname()[0].lower()
        dist = platform.dist()[0].lower()
        rootdir = experimentConfig.get('expdl', {}).get('distributionPath', sys.path[0])
    
        # Try our local prebuilt software first, this gets around dist installers pointing outside the testbed and long timeouts
        softwareConfig.append({'type': 'rpmfile', 'dir': os.path.join(rootdir, getArch())})
        softwareConfig.append({'type': 'archive', 'dir': os.path.join(rootdir, getArch())})
    
        # then dist installer
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
     
    mesdl = experimentConfig.get('mesdl')
    log.debug("Mesdl from experiment wide configuration: %s", mesdl)
    
    if not transportsConfig:
        nodename_control = testbed.fqdn
        # For each external connection, add a TCPServer transport    
        for bridge in mesdl['bridges']:
            log.debug("Bridge: %s", bridge)
            if nodename_control == bridge['server']:
                # A transport server is added on the node  
                # This is used to provide an external facing connection to the magi messaging network on port extport (typically 18808)  
                transportsConfig.append({ 'class': bridge['type'], 'address': '0.0.0.0', 'port': bridge['port']})
        
        # For each messaging overlay that the local node is part of, 
        # Add an apporpriate transport 
        # NOTE: We are just adding TCPTransports currently 
        for t in mesdl['overlay']:
            log.debug("Control Plane Overlay: %s", t)
            if t['type'] == 'TCPTransport' and nodename_control != t['server'] and (nodename_control in t['members'] or '__ALL__' in t['members']):
                server_name = t['server']
                # DETER/emulab DNS will resolves FQDNs to the control network address, 
                # A FQDN or IP address would be required for connecting with the external world 
                try:
                    server_addr=gethostbyname(server_name)
                except gaierror:
                    log.critical('Using MeSDL file %s\n Unable to resolve node name %s, EXITING', mesdl, server_name)
                    sys.exit(2)
                        
                transportsConfig.append({ 'class': 'TCPTransport', 'address': server_addr, 'port': t['port'] })
                
            elif t['type'] == 'MulticastTransport' and (nodename_control in t['members'] or '__ALL__' in t['members']):
                transportsConfig.append({ 'class': 'MulticastTransport', 'address': t['address'], 'localaddr': testbed.controlip, 'port': t['port'] })


    #Database Configuration
    databaseConfig = nodeConfig.setdefault('database', {})
    
    dbdl = experimentConfig.get('dbdl')
    log.debug("Dbdl from experiment wide configuration: %s", dbdl)
    
    if localInfo['nodename'] not in experimentConfig['expdl']['magiNodeList']:
        databaseConfig['isDBEnabled'] = False
    isDBEnabled = databaseConfig.setdefault('isDBEnabled', dbdl.get('isDBEnabled'))
    if isDBEnabled:
        isDBSharded = databaseConfig.setdefault('isDBSharded', dbdl.get('isDBSharded'))
        databaseConfig.setdefault('sensorToCollectorMap', dbdl['sensorToCollectorMap'])
        sensorToCollectorMap = databaseConfig['sensorToCollectorMap']
        validateSensorToColletorMap(sensorToCollectorMap, 
                                    experimentConfig['expdl']['magiNodeList'])
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
