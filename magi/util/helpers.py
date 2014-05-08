import logging
import os
import errno
import platform
import ctypes
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
