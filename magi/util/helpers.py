from magi.testbed import testbed
import ctypes
import errno
import logging
import os
import platform
import yaml

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
        
def loadYaml(filename):
    """ Load the configuration data from file """
    fp = open(filename, 'r')
    data = yaml.load(fp)
    fp.close()
    return data

def toSet(value):
    if type(value) is list:
        value = set(value)
    elif type(value) is str:    
        value= set([s.strip() for s in value.split(',')])
    elif value is None:
        value= set()
    return value

def is_os_64bit():
        return platform.machine().endswith('64')
    
def getThreadId():
    if platform.system() == 'Linux':
        if platform.architecture()[0] == '64bit':
            return ctypes.CDLL('libc.so.6').syscall(186)
        else:
            return ctypes.CDLL('libc.so.6').syscall(224)
        
    return -1

def readPropertiesFile(filename):
    import ConfigParser
    import io
    parser = ConfigParser.RawConfigParser()
    properties = '[root]\n' + open(filename, 'r').read()
    parser.readfp(io.BytesIO(properties))
    kv_pairs = parser.items('root')
    return dict(kv_pairs)

def toControlPlaneNodeName(nodename):
    if nodename not in ['localhost', '127.0.0.1'] and '.' not in nodename:
        nodename += '.%s.%s' % (testbed.getExperiment(), testbed.getProject())
    return nodename

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
