
"""
Example experiment configuration file 

mesdl:
  bridges:
  - {server: node-1.myExperiment.myProject, type: TCPServer, port: 18808}
  - {server: node-1.myExperiment.myProject, type: TCPServer, port: 28808}
  overlay:
  - members: [__ALL__]
    port: 28808
    server: node-1.myExperiment.myProject
    type: TCPTransport

dbdl:
  isDBEnabled: true
  configHost: node-1
  collectorMapping: {node-1: node-1, node-2: node-2}

expdl:
  eid: myExperiment
  pid: myProject
  magiDistDir: /share/magi/current/

"""

"""
  Example node configuration file:

database:
  isDBEnabled: true
  isConfigHost: true
  isCollector: true
  isSensor: true
  collectorMapping: {node-1: node-1, node-2: node-2}
  collector: node-1
  configHost: node-1

localInfo:
  architecture: 64bit
  controlif: eth0
  controlip: 192.168.0.95
  distribution: Ubuntu 12.04 (precise)
  hostname: node-1.myExperiment.myProject.isi.deterlab.net
  interfaceInfo:
    10.1.1.2: {expif: eth4, expip: 10.1.1.2, expmac: a0369f0927f2, linkname: link}
  nodename: node-1
  tempdir: /tmp

software:
- {dir: /share/magi/dev/Linux-Ubuntu12.04-x86_64, type: rpmfile}
- {dir: /share/magi/dev/Linux-Ubuntu12.04-x86_64, type: archive}
- {type: apt}
- {dir: /share/magi/dev/source, type: source}
- {dir: /tmp/src, type: source}

transports:
   - { class: MulticastTransport, address: 239.255.1.2, localaddr: 192.168.1.1, port: 18808 }
   - { class: TCPTransport, address: 192.168.2.1, port: 18808 }
   - { class: TCPServer, address: 0.0.0.0, port: 18808 }
   - { class: SSLTransport, address: 172.16.1.1, port: 18810, cafile: /proj/P/exp/E/tbdata/ca.pem, nodefile: /proj/P/exp/E/tbdata/node.pem, matchingOU: P.E }
   - { class: SSLServer, address: 0.0.0.0, port: 18810, cafile: /proj/P/exp/E/tbdata/ca.pem, nodefile: /proj/P/exp/E/tbdata/node.pem, matchingOU: P.E }
   - { class: TextPipe, filename: /var/tmp/mypipe, src: mynode srcdock: mydock dstgroups: ['groupa'], dstdocks: ['docka']  }

"""


from execl import run
from magi.testbed import testbed
from socket import gethostbyname, gaierror
import Queue
import datetime
import logging
import os
import platform
import subprocess
import sys
import yaml
import helpers

DEFAULT_DIR   = "/var/log/magi" 

LOG_FILE      = os.path.join(DEFAULT_DIR, "daemon.log")
EXPCONF_FILE  = os.path.join(DEFAULT_DIR, "experiment.conf")
NODECONF_FILE = os.path.join(DEFAULT_DIR, "node.conf")
MAGIPID_FILE  = os.path.join(DEFAULT_DIR, "magi.pid")

nodeConfig = dict()
experimentConfig = dict()

log = logging.getLogger(__name__)

def getExperimentConfig():
    global experimentConfig
    if not experimentConfig:
        try:
            experimentConfig = helpers.loadYaml(EXPCONF_FILE)
        except:
            log.exception("Could not load experiment configuration from %s", EXPCONF_FILE)
            raise
    return experimentConfig

def getNodeConfig():
    global nodeConfig
    if not nodeConfig:
        try:
            nodeConfig = helpers.loadYaml(NODECONF_FILE)
        except:
            log.exception("Could not load node configuration from %s", NODECONF_FILE)
            raise
    return nodeConfig

def getConfig():
    return getNodeConfig()

def loadExperimentConfig(expConfigFile=EXPCONF_FILE):
    """ Load the experiment-wide configuration data from file, filename can be overriden """
    global experimentConfig
    experimentConfig = helpers.loadYaml(expConfigFile)
    return experimentConfig

def loadNodeConfig(nodeConfigFile=NODECONF_FILE):
    """ Load the node specific configuration data from file, filename can be overriden """
    global nodeConfig
    nodeConfig = helpers.loadYaml(nodeConfigFile)
    return nodeConfig
    
def createExperimentConfig(magiDistDir, isDBEnabled):
    log.info("Creating experiment wide configuration file.....") 
    fp = open(EXPCONF_FILE, 'w')
    expConf = dict()
    expConf['mesdl'] = getDefaultMESDL()
    expConf['dbdl']  = getDefaultDBDL(isDBEnabled)
    expConf['expdl'] = getDefaultEXPDL(magiDistDir)
    fp.write(yaml.safe_dump(expConf))
    fp.close()
    return EXPCONF_FILE 

def getDefaultMESDL():
    """ Create a default mesdl for the control plane """
    log.info("Creating control plane mesdl") 
    controlNode = testbed.getServer() 
    log.info("Using %s as control node....", controlNode) 
    if not '.' in controlNode:
        controlNode += '.%s.%s' % (testbed.getExperiment(), testbed.getProject())
    mesdl = dict()
    mesdl['bridges'] = list()
    mesdl['overlay'] = list()
    mesdl['bridges'].append({ 'type': 'TCPServer', 'server':controlNode, 'port': 18808 })
    transportClass = 'TCP'
    if transportClass == 'TCP':
        mesdl['bridges'].append({ 'type': 'TCPServer', 'server':controlNode, 'port': 28808 })
        mesdl['overlay'].append({ 'type': 'TCPTransport', 'members': ['__ALL__'], 'server':controlNode, 'port': 28808 })
    elif transportClass == 'Multicast':
        mesdl['overlay'].append({ 'type': 'MulticastTransport', 'members': ['__ALL__'], 'address': getMulticast(testbed.project, testbed.experiment, 0), 'port': 28808 })
    return mesdl

def getDefaultDBDL(isDBEnabled=True):
    """ Create a default db configuration file """
    log.info("Creating db config file.....")
    dbdl = dict()
    dbdl['isDBEnabled'] = isDBEnabled
    if isDBEnabled:
        dbdl['collectorMapping'] = dict()
        topoGraph = testbed.getTopoGraph()
        for node in topoGraph.nodes():
            dbdl['collectorMapping'][node] = node
        dbdl['configHost'] = testbed.getServer()
    return dbdl

def getDefaultEXPDL(magiDistDir='share/magi/current/'):
    """ """
    expdl = dict()
    expdl['magiDistDir'] = magiDistDir
    expdl['pid'] = testbed.getProject()
    expdl['eid'] = testbed.getExperiment()
    return expdl
    
def createNodeConfig(experimentConfigFile=EXPCONF_FILE, nodeConfigFile=NODECONF_FILE):
    """
        Create a per experiment node magi configuration file
    """
    
    experimentConfig = helpers.loadYaml(experimentConfigFile)
    
    # Information about the local node for reference 
    localInfo = dict()
    localInfo['nodename'] = testbed.nodename
    localInfo['hostname'] = platform.uname()[1]
    localInfo['distribution'] = str(platform.dist()[0]+" "+platform.dist()[1]+" ("+platform.dist()[2]+")")
    localInfo['architecture'] = platform.architecture()[0]
    localInfo['controlip'] = testbed.controlip
    localInfo['controlif'] = testbed.controlif
    localInfo['tempdir'] = '/tmp'
    
    interfaceInfo = dict()
    topoGraph = testbed.getTopoGraph()
    # Would it be possible to write the link name the interface is associated with? 
    for ip in testbed.getLocalIPList():
        linkname = 'unknown'
        for link in topoGraph.node[testbed.nodename]['links']:
            if link['ip'] == ip:
                linkname = link['name']
        interfaceInfo[ip] = { 'expip': ip, 'expif': testbed.getInterfaceInfo(ip).name, 'expmac': testbed.getInterfaceInfo(ip).mac, 'linkname': linkname }
    
    localInfo['interfaceInfo'] = interfaceInfo
    #config['processAgentsCommPort'] = None
    
    # Information about the location of the software libraries 
    softwareConfig = list()

    osname = platform.uname()[0].lower()
    dist = platform.dist()[0].lower()
    rootdir = experimentConfig.get('expdl', {}).get('magiDistDir', sys.path[0])

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
    mesdl = experimentConfig.get('mesdl')
    log.info("Mesdl from experiment wide configuration file %s: %s", experimentConfig, mesdl)
    
    transports = list()
    
    nodename_control = testbed.nodename + '.%s.%s' % (testbed.getExperiment(), testbed.getProject())
    # For each external connection, add a TCPServer transport    
    for bridge in mesdl['bridges']:
        log.debug("Bridge: %s", bridge)
        if nodename_control == bridge['server']:
            # A transport server is added on the node  
            # This is used to provide an external facing connection to the magi messaging network on port extport (typically 18808)  
            transports.append({ 'class': bridge['type'], 'address': '0.0.0.0', 'port': bridge['port']})
    
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
                    
            transports.append({ 'class': 'TCPTransport', 'address': server_addr, 'port': t['port'] })
            
        elif t['type'] == 'MulticastTransport' and (nodename_control in t['members'] or '__ALL__' in t['members']):
            transports.append({ 'class': 'MulticastTransport', 'address': t['address'], 'localaddr': testbed.controlip, 'port': t['port'] })

    dbdl = experimentConfig.get('dbdl')   
    dbInfo = dict()
    if dbdl.get('isDBEnabled'):
        dbInfo['isDBEnabled'] = True
        dbInfo['isConfigHost'] = (testbed.nodename == dbdl['configHost'])
        dbInfo['isCollector'] = (testbed.nodename in dbdl['collectorMapping'].values())
        dbInfo['isSensor'] = (testbed.nodename in dbdl['collectorMapping'].keys() or '__ALL__' in dbdl['collectorMapping'].keys())
        dbInfo['configHost'] = dbdl['configHost']
        dbInfo['collector'] = dbdl['collectorMapping'].get(testbed.nodename, dbdl['collectorMapping'].get('__ALL__'))
        dbInfo['collectorMapping'] = dbdl['collectorMapping']
    else:
        dbInfo['isDBEnabled'] = False
        
    config = dict()
    config['localInfo'] = localInfo
    config['software'] = softwareConfig
    config['transports'] = transports
    config['database'] = dbInfo
    
    fp = open(nodeConfigFile, 'w') 
    fp.write(yaml.safe_dump(config))
    fp.close()
    
    return nodeConfigFile

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

chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
def getMulticast(arg1, arg2, channel):
    return "239.255.%d.%d" % (_str2byte(arg1), (_str2byte(arg2)+channel)%255)
def _intval(x, y):
    return x + chars.find(y)
def _str2byte(strin):
    return reduce(_intval, strin, 0) % 255


def checkAndCorrectExperimentConfig(experimentConfigFile=EXPCONF_FILE):
    try:
        experimentConfig = helpers.loadYaml(experimentConfigFile)   
    except Exception, e:
        log.exception("Exception while reading config file")
        experimentConfig = {}
        
    mesdl = experimentConfig.get('mesdl', {})
    dbdl = experimentConfig.get('dbdl', {})
    expdl = experimentConfig.get('expdl', {})
    
    experimentConfig['mesdl'] = validateMesDL(mesdl)
    experimentConfig['dbdl'] = validateDBDL(dbdl)
    
    fp = open(experimentConfigFile, 'w')
    fp.write(yaml.safe_dump(experimentConfig))
    fp.close()
    
def validateMesDL(mesdl={}):
    if not mesdl:
        return getDefaultMESDL()
    return mesdl
    
def validateDBDL(dbdl={}):
    """ Checking if a valid db config exists """
    '''
        isDBEnabled: true
        configHost: node-1
        collectorMapping: {node-1: node-1, node-2: node-2}
    '''
    
    if not dbdl:
        return getDefaultDBDL()
    
    isDBEnabled = dbdl.get('isDBEnabled', True)
    dbdl['isDBEnabled'] = isDBEnabled
    
    if isDBEnabled:
        if "collectorMapping" not in dbdl.keys() or not dbdl['collectorMapping']:
            return getDefaultDBDL()
            
        experimentNodes = topoGraph.nodes()
        
        for (sensor, collector) in dbdl['collectorMapping'].iteritems():
            if sensor not in experimentNodes:
                del dbdl['collectorMapping'][sensor]
            elif collector not in experimentNodes:
                dbdl['collectorMapping'][sensor] = sensor
        
        if dbdl.get('configHost') not in experimentNodes:
            dbdl['configHost'] = testbed.getServer()
    
    else:
        dbdl = {}
        dbdl['isDBEnabled'] = False
        
    return dbdl


def verifyConfig( magiconf=None ):
    """ Make sure there is a default config file and that its not some blank thing from an error """
            
    if not magiconf:
        magiconf=MAGICONF_FILE 

    if os.path.exists(magiconf) and os.path.getsize(magiconf) > 100:
        return True 


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
