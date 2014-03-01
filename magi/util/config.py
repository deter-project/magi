"""" 
  Example config file:

software:
   - { type: yum }
   - { type: rpmfile, dir: /share/magi/v20/Linux-Ubuntu10.04-i686 }
   - { type: archive, dir: /share/magi/v20/Linux-Ubuntu10.04-i686 }
   - { type: source, dir: /share/magi/v20/source }

transports:
   - { class: MulticastTransport, address: 239.255.1.2, localaddr: 192.168.1.1, port: 18808 }
   - { class: TCPTransport, address: 192.168.2.1, port: 18808 }
   - { class: TCPServer, address: 0.0.0.0, port: 18808 }
   - { class: SSLTransport, address: 172.16.1.1, port: 18810, cafile: /proj/P/exp/E/tbdata/ca.pem, nodefile: /proj/P/exp/E/tbdata/node.pem, matchingOU: P.E }
   - { class: SSLServer, address: 0.0.0.0, port: 18810, cafile: /proj/P/exp/E/tbdata/ca.pem, nodefile: /proj/P/exp/E/tbdata/node.pem, matchingOU: P.E }
   - { class: TextPipe, filename: /var/tmp/mypipe, src: mynode srcdock: mydock dstgroups: ['groupa'], dstdocks: ['docka']  }

"""


"""
Example: mesdl file 

bridges:
- { TCPServer: node3, port: 18808 }
- { TCPServer: node3, port: 38808 } 
overlay:
- { type: TCPServer, server: node2, port: 28808 } 
- { type: TCPTransport, members: [ '__ALL__' ], server: node2, port: 28808 } 
- { type: TCPTransport, members: [ 'node4', 'node7' ], server: node2, port: 28808 } 

"""


from execl import run
from magi.testbed import testbed
from socket import gethostbyname, gaierror
import Queue
import datetime
import errno
import logging
import os
import platform
import subprocess
import sys
import yaml
import ctypes

MAGILOG="/var/log/magi" 

DEFAULT_MAGICONF=MAGILOG+"/magi.conf"
DEFAULT_MAGIPID=MAGILOG+"/magi.pid" 
DEFAULT_EXPMESDL=MAGILOG+"/mesdl.conf"
DEFAULT_KEYDIR="/proj/%s/exp/%s/tbdata/" % (testbed.project, testbed.experiment)
DEFAULT_DBCONF=MAGILOG+"/db.conf"

# logging.basicConfig(filename = MAGILOG +'/daemon.log', level=logging.INFO)
config = dict()
log = logging.getLogger(__name__)

def makeDir(name):
    try:
        os.mkdir(name)
    except OSError, e:
        if e.errno == errno.EEXIST: return
        log.warning("Couldn't create FIFO dir: %s", e)

def makePipe(name):
    try:
        os.mkfifo(name)
    except OSError, e:
        if e.errno == errno.EEXIST: return
        log.warning("Couldn't create FIFO file: %s, %s", name, e)

def toDirected(graph, root):
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

def getConfig():
    global config
    if not config:
        config = loadConfig()
    return config

def loadConfig(filename=None):
    """ Load the configuration data from file, filename can be overriden """
    global config
    if filename is None:
        filename = DEFAULT_MAGICONF 
    fp = open(filename, 'r')
    config = yaml.load(fp)
    fp.close()
    return config

def loadYaml(filename):
    """ Load the configuration data from file """
    fp = open(filename, 'r')
    data = yaml.load(fp)
    fp.close()
    return data

def createMESDL():
    """ Create a default mesdl with one shared TCP based overlay and one external server """
    log.info("Creating mesdl file.....") 
    fp = open(DEFAULT_EXPMESDL, 'w')
    mesdl = createMESDL_control()
    fp.write(yaml.safe_dump(mesdl))
    mesdl_exp = createMESDL_experiment()
    fp.write(yaml.safe_dump(mesdl_exp))
    fp.close()
    return DEFAULT_EXPMESDL  

def createMESDL_control():
    """ Create a default mesdl for the control plane """
    log.info("Creating control plane mesdl") 
    node = testbed.getServer() 
    log.info("Using %s as control node....", node) 
    if not '.' in node:
        node += '.%s.%s' % (testbed.getExperiment(), testbed.getProject())
    mesdl = dict()
    mesdl['bridges'] = list()
    mesdl['bridges'].append({ 'TCPServer': node, 'port': 18808 })  
    mesdl['bridges'].append({ 'TCPServer': node, 'port': 28808 })  
    mesdl['overlay'] = list()
    memlist = list()
    memlist.append('__ALL__')
    mesdl['overlay'].append({ 'type': 'TCPTransport' , 'members': memlist, 'server':node, 'port': 28808 })
    return mesdl  

def createMESDL_experiment():
    """ Create a default mesdl for the experiment plane """
    log.info("Creating experiment plane mesdl") 
    root = testbed.getServer() 
    log.info("Using %s as root node....", root) 
    graph = testbed.getTopoGraph()
    d = toDirected(graph, root)
    mesdl = dict()
    mesdl['bridges_exp'] = list()
    mesdl['overlay_exp'] = list()
    transportClass = 'TCP'
    if transportClass == 'TCP':
        for node in d.nodes():
            if d.out_degree(node) != 0:
                mesdl['overlay_exp'].append({ 'type': 'TCPServer', 'server': node, 'port': 48808 })
                mesdl['overlay_exp'].append({ 'type': 'TCPTransport', 'members': d.successors(node), 'server': node, 'port': 48808 })
    
    elif transportClass == 'Multicast':
        mesdl['overlay_exp'].append({ 'type': 'MulticastTransport', 'address': getMulticast(testbed.project, testbed.experiment, 0), 'port': 48808 })
    mesdl['bridges_exp'].append({ 'TCPServer': root, 'port': 38808 }) 
    return mesdl  

def validateDBConf(dbconf=None):
    """ Chekcing if a valid db config exists """
    if dbconf:
        expdbconf = loadYaml(dbconf)
    else:
        expdbconf = dict()
        
    if "collector" not in expdbconf.keys() or not expdbconf['collector']:
        expdbconf['collector'] = dict()
        
    topoGraph = testbed.getTopoGraph()
    sensors = expdbconf['collector'].keys()
    for node in topoGraph.nodes():
        if node not in sensors or expdbconf['collector'][node] not in topoGraph.nodes():
            expdbconf['collector'][node] = node
        
    for sensor in sensors:
        if sensor not in topoGraph.nodes():
            del expdbconf['collector'][sensor]
            
    if "queriers" not in expdbconf.keys() or not expdbconf['queriers']:
        expdbconf['queriers'] = []
        
    queriers = expdbconf['queriers']
    for querier in queriers:
        if querier not in topoGraph.nodes():
            expdbconf['queriers'].remove(querier)
            
    fp = open(DEFAULT_DBCONF, 'w')
    fp.write(yaml.safe_dump(expdbconf))
    fp.close()
    
    return DEFAULT_DBCONF

def createDBConf():
    """ Create a default db configuration file """
    log.info("Creating db config file.....")
    fp = open(DEFAULT_DBCONF, 'w')
    dbconf = dict()
    dbconf['collector_mapping'] = dict()
    topoGraph = testbed.getTopoGraph()
    for node in topoGraph.nodes():
        dbconf['collector_mapping'][node] = node
    fp.write(yaml.safe_dump(dbconf))
    fp.close
    return DEFAULT_DBCONF

def keysExist(project=None, experiment=None, keydir=None):
    """
        Simple check to see if the specific keys exist before creating them
    """
    project = project or testbed.project
    experiment = experiment or testbed.experiment
    if keydir is None:
        keydir = DEFAULT_KEYDIR
    cafile = os.path.join(keydir, 'ca.pem')
    nodefile = os.path.join(keydir, 'node.pem')
    subprocess.call("ls -l %s > /dev/null" % (keydir), shell=True)  # ugly mans way of flushing NFS read cache
    if not os.path.exists(cafile) or not os.path.exists(nodefile):
        return False
    return True


def generateKeys(project=None, experiment=None, keydir=None):
    """
        Generate the ca.pem and node.pem files for the specified project/experiment. If
        keydir is specified, they are placed there.  If not, they are placed in
        /proj/P/exp/E/tbdata/
    """
    project = project or testbed.project
    experiment = experiment or testbed.experiment
    if keydir is None:
        keydir = DEFAULT_KEYDIR
    cafile = os.path.join(keydir, 'ca.pem')
    nodefile = os.path.join(keydir, 'node.pem')
    ou = "%s.%s" % (project, experiment)
    # find the number of days to the end of 'time' and use that
    days = (datetime.date.fromtimestamp(0x7fffffff) - datetime.date.today()).days - 10

    run("echo \"03\" > ca.serial", shell=True)
    run("openssl genrsa -out ca.key 1024", shell=True)
    run("openssl req -new -x509 -key ca.key -out %s -days %d -subj \"/C=US/ST=CA/O=DETER/OU=%s/CN=CA\"" % (cafile, days, ou), shell=True)
    run("openssl genrsa -out exp.key 1024", shell=True)
    run("openssl req -new -key exp.key -out exp.req -subj \"/C=US/ST=CA/O=DETER/OU=%s/CN=node\"" % (ou), shell=True)
    run("openssl x509 -CA %s -CAkey ca.key -CAserial ca.serial -req -in exp.req -out exp.signed -days %d" % (cafile, days), shell=True)
    run("openssl pkcs8 -nocrypt -in exp.key -topk8 -outform der -out node.pk8", shell=True)
    run("cat exp.signed exp.key > %s" % (nodefile), shell=True)
    run("rm exp.signed exp.key exp.req ca.serial ca.key node.pk8", shell=True)


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

def getThreadId():
    if platform.system() == 'Linux':
        if platform.architecture()[0] == '64bit':
            return ctypes.CDLL('libc.so.6').syscall(186)
        else:
            return ctypes.CDLL('libc.so.6').syscall(224)
        
    return -1
    

chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
def getMulticast(arg1, arg2, channel):
    return "239.255.%d.%d" % (_str2byte(arg1), (_str2byte(arg2)+channel)%255)
def _intval(x, y):
    return x + chars.find(y)
def _str2byte(strin):
    return reduce(_intval, strin, 0) % 255

def createConfig(mesdl=DEFAULT_EXPMESDL, dbconf=DEFAULT_DBCONF, magiconf=DEFAULT_MAGICONF, rootdir=None, enable_dataman=False):
    """
        Create a per experiment node magi configuration file
        rootdir - for the magi code distribution 
        keydir - location of key dir if specifying SSL Transport.
    """
    
    global config
    
    keydir = None

#    # Information about the local node for reference 
#    config['localinfo'] = list()
#    config['localinfo'].append({ 'nodename': testbed.nodename })
#    config['localinfo'].append({ 'hostname': platform.uname()[1] })
#    config['localinfo'].append({ 'distribution': str(platform.dist()[0]+" "+platform.dist()[1]+" ("+platform.dist()[2]+")") }) 
#    config['localinfo'].append({ 'controlip': testbed.controlip , 'controlif': testbed.controlif })
#    # Would it be possible to write the link name the interface is associated with? 
#    for ip in testbed.getLocalIPList():
#        config['localinfo'].append({ 'expip': ip, 'expif': testbed.getInterfaceInfo(ip).name, 'expmac': testbed.getInterfaceInfo(ip).mac })

    # Information about the local node for reference 
    localinfo = dict()
    localinfo['nodename'] = testbed.nodename
    localinfo['hostname'] = platform.uname()[1]
    localinfo['distribution'] = str(platform.dist()[0]+" "+platform.dist()[1]+" ("+platform.dist()[2]+")")
    localinfo['architecture'] = platform.architecture()[0]
    localinfo['controlip'] = testbed.controlip
    localinfo['controlif'] = testbed.controlif
    
    interfaceInfo = dict()
    topoGraph = testbed.getTopoGraph()
    # Would it be possible to write the link name the interface is associated with? 
    for ip in testbed.getLocalIPList():
        linkname = 'unknown'
        for link in topoGraph.node[testbed.nodename]['links']:
            if link['ip'] == ip:
                linkname = link['name']
        interfaceInfo[ip] = { 'expip': ip, 'expif': testbed.getInterfaceInfo(ip).name, 'expmac': testbed.getInterfaceInfo(ip).mac, 'linkname': linkname }
    
    localinfo['interfaceInfo'] = interfaceInfo
    config['localinfo'] = localinfo
    
    config['processAgentsCommPort'] = None
    
    # Information about the location of the software libaries 
    config['software'] = list()
    if keydir is None:
        keydir = DEFAULT_KEYDIR

    osname = platform.uname()[0].lower()
    dist = platform.dist()[0].lower()
    if rootdir is None:
        rootdir = sys.path[0]

    # Try our local prebuilt software first, this gets around dist installers pointing outside the testbed and long timeouts
    config['software'].append({ 'type': 'rpmfile', 'dir': os.path.join(rootdir, getArch())})
    config['software'].append({ 'type': 'archive', 'dir': os.path.join(rootdir, getArch())})

    # then dist installer
    if dist in ('ubuntu', 'debian'):
        config['software'].append({ 'type': 'apt'})
    elif dist in ('redhat', 'fedora'):
        config['software'].append({ 'type': 'yum'})
    elif osname in ('freebsd', ):
        config['software'].append({ 'type': 'pkgadd'})

    # then build as last resort
    config['software'].append({ 'type': 'source',  'dir': os.path.join(rootdir, 'source')})
    config['software'].append({ 'type': 'source',  'dir': os.path.join('/tmp/src')})

        
    # Infomation about the transports  
    # REad the messaging overlay description for the experiment and create 
    # the required transports for this node 
    expmesdl = loadYaml(mesdl)    
    log.info("Mesdl for file %s: %s", mesdl, expmesdl)
    
    config['transports'] = list()
    
    nodename_control = testbed.nodename + '.%s.%s' % (testbed.getExperiment(), testbed.getProject())
    # For each external connection, add a TCPServer transport    
    for bridge in expmesdl['bridges']:
        log.debug("Bridge: %s", bridge)
        if nodename_control == bridge['TCPServer']:
            # A TCP server service is added on the node  
            # This is used to provide an external facing connection to the magi messaging network on port extport (typically 18808)  
            config['transports'].append({ 'class': 'TCPServer', 'address': '0.0.0.0', 'port': bridge['port']})
    
    # For each messaging overlay that the local node is part of, 
    # Add an apporpriate transport 
    # NOTE: We are just adding TCPTransports currently 
    for t in expmesdl['overlay']:
        log.debug("Control Plane Overlay: %s", t)
        if t['type'] == 'TCPServer' and nodename_control == t['server']:
            config['transports'].append({ 'class': 'TCPServer', 'address': '0.0.0.0', 'port': t['port']})
            
        elif t['type'] == 'TCPTransport' and nodename_control != t['server'] and (nodename_control in t['members'] or t['members'][0] == '__ALL__'):
            server_name = t['server']
            # DETER/emulab DNS will resolves FQDNs to the control network address, 
            # A FQDN or IP address would be required for connecting with the external world 
            try:
                server_addr=gethostbyname(server_name)
            except gaierror:
                log.critical('Using MeSDL file %s\n Unable to resolve node name %s, EXITING', mesdl, server_name)
                sys.exit(2)
                    
            config['transports'].append({ 'class': 'TCPTransport', 'address': server_addr, 'port': t['port'] })
            
        elif t['type'] == 'MulticastTransport':
            config['transports'].append({ 'class': 'MulticastTransport', 'address': t['address'], 'localaddr': testbed.controlip, 'port': t['port'] })

    config['transports_exp'] = list()
    
    for t in expmesdl['overlay_exp']:
        log.debug("Experiment Plane Overlay: %s", t)
        if t['type'] == 'TCPServer' and testbed.nodename == t['server']:
            config['transports_exp'].append({ 'class': 'TCPServer', 'address': '0.0.0.0', 'port': t['port']})
            
        elif t['type'] == 'TCPTransport' and testbed.nodename != t['server'] and (testbed.nodename in t['members'] or t['members'][0] == '__ALL__'):
            server_name = t['server']
            # DETER/emulab DNS will resolves FQDNs to the control network address, 
            # A FQDN or IP address wauld be required for connecting with the external world 
            try:
                server_addr=gethostbyname(server_name)
            except gaierror:
                log.critical('Using MeSDL file %s\n Unable to resolve node name %s, EXITING', mesdl, server_name)
                sys.exit(2)
                    
            config['transports_exp'].append({ 'class': 'TCPTransport', 'address': server_addr, 'port': t['port'] })
            
        elif t['type'] == 'MulticastTransport':
            config['transports_exp'].append({ 'class': 'MulticastTransport', 'address': t['address'], 'localaddr': testbed.controlip, 'port': t['port'] })
            
    if hasattr(testbed, 'getTextPipes'):
        for name in testbed.getTextPipes():
            filename = '/var/run/magipipes/%s.pipe'%name
            config['transports'].append({
                'type':'TestPipe',
                'filename':filename,
                'src':name,
                'srcdock':'worm',
                'dstgroups': ['data'],
                'dstdocks': ['data', 'worm']
            })

    config['tempdir'] = '/tmp'
    
    if enable_dataman:
        config['isDatamanSetup'] = True
        expdbconf = loadYaml(dbconf)
        config['collector_mapping'] = expdbconf['collector_mapping']
        config['queriers'] = set(expdbconf['queriers']) if 'queriers' in expdbconf else set()
        config['dbhost'] = expdbconf['collector_mapping'][testbed.nodename]
        config['isDBHost'] = (expdbconf['collector_mapping'][testbed.nodename] == testbed.nodename)
        
        if testbed.nodename in config['queriers']:
            config['transports'].append({ 'class': 'TCPServer', 'address': '0.0.0.0', 'port': 18808})
    else:
        config['isDatamanSetup'] = False
        
    fp = open (magiconf, 'w') 
    fp.write(yaml.safe_dump(config))
    fp.close()
    return True 


def verifyConfig( magiconf=None ):
    """ Make sure there is a default config file and that its not some blank thing from an error """
            
    if not magiconf:
        magiconf=DEFAULT_MAGICONF 

    if os.path.exists(magiconf) and os.path.getsize(magiconf) > 100:
        return True 

if __name__ == "__main__":
    print "In main" 
    createMESDL() 
    createConfig()
