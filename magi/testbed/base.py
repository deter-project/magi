# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import re
import socket

from magi.util.execl import pipeIn
from magi.util import helpers

def NIE():
    """ 
        Throw exception that something wasn't implemented
    """
    raise NotImplementedError("Subclass did not implement this method")


class IFObj(object):
    """ 
        Simple struct for maintining a tuple of interface information but with access
        by name
    """
    def __init__(self, ip, name, mac, mask):
        self.ip = ip
        self.name = name
        self.mac = mac
        self.mask = mask

    def __repr__(self):
        return "name=%s, ip=%s, mask=%s, mac=%s" % (self.name, self.ip, self.mask, self.mac)


class Testbed(object):
    """
        Base testbed class that provides the 'interface' that user expect as well as
        providing property access for certain values
    """

    def __init__(self):
        pass

    @property
    def nodename(self):
        """ local node name """
        return self.getNodeName()
    
    @property
    def server(self):
        """ server node name """
        return self.getServer()
    
    @property
    def fqdn(self):
        """ local node fully qualified doamin name"""
        return self.getFQDN()
    
    @property
    def controlip(self):
        """ local control interface IP address """
        return self.getControlIP()

    @property
    def controlif(self):
        """ local control interface name """
        return self.getControlIF()

    @property
    def topograph(self):
        """ experiment topology graph """
        return self.getTopoGraph()

    """    Testbed Properties (readonly) """
    
    def getServer(self, FQDN=False):
        # Gets the complete topology map and returns control if 
        # if it finds a node named "control" otherwise it returns 
        # the first node in the alpha-numerically sorted list  
        topoGraph = self.getTopoGraph()
        nodes = topoGraph.nodes()
        if not nodes:
            return 'Unknown'
        nodes.sort()
        host = nodes[0]
        for node in nodes:
            if 'control' == node.lower():
                host = 'control'
                break
        
        if FQDN:
            return '%s.%s.%s' % (host, self.experiment, self.project)
        else:
            return host    
    
    def getExperimentDir(self):
        NIE()
        
    def toControlPlaneNodeName(self, nodename):
        NIE()

    def getTopoGraph(self):
        if 'topograph' not in self._store:
            self.loadTopoGraph()
        return self._store['topograph']
    
    """ Local node properties (readonly) """

    def getNodeName(self):
        if 'node' not in self._store:
            self.loadEID()
        return self._store['node']

    def getFQDN(self):
        return "%s.%s.%s" %(self.nodename, self.experiment, self.project)
    
    def getControlIP(self):
        if 'controlip' not in self._store:
            self.loadControlInfo()
        return self._store['controlip']

    def getControlIF(self):
        if 'controlif' not in self._store:
            self.loadControlInfo()
        return self._store['controlif']


    """ Queries for this Node """

    def getLocalIPList(self):
        """returns list of IP """
        if 'iflist' not in self._store:
            self.loadIfConfig()
        return [obj.ip for obj in self._store['iflist']]

    def getLocalIFList(self):
        """returns list of IF """
        if 'iflist' not in self._store:
            self.loadIfConfig()
        return [obj.name for obj in self._store['iflist']]

    def getInterfaceList(self):
        """ return the list of IFObj's """
        if 'iflist' not in self._store:
            self.loadIfConfig()
        return self._store['iflist']
        
    def getInterfaceInfo(self, matchip=None, matchname=None):
        """ return IFObj for ip or name"""
        if not matchip and not matchname:
            raise KeyError("Either IP or interface name should be provided")
        if 'iflist' not in self._store:
            self.loadIfConfig()
        if matchip:
            for i in self._store['iflist']:
                if i.ip == matchip:
                    return i
        for i in self._store['iflist']:
            if i.name == matchname:
                return i
        raise KeyError("Invalid IP or interface name provided.")
    
    """ Queries for other Experiment Info """

    def getLocalVirtualNodes(self):
        """ Get all the virtual nodes hosted by this machine """
        NIE()

    def amAVirtualNode(self):
        """ return true if I am a virtual node (i.e. not a physical node or virtual host) """
        NIE()

    def readFirstLine(self, fp):
        """ 'macro' for reading the first line of a file pointer and closing """
        line = fp.readline().strip()
        fp.close()
        return line

    def readAllLines(self, fp):
        """ 'macro' for reading all lines of a file pointer and closing """
        lines = fp.readlines()
        fp.close()
        return lines
    
    def getIfconfigData(self, matchip=None, matchname=None):
        """ Get the name and MAC address for an interface given its IP address, IP can be a regular expression """
        if not matchip and not matchname:
            raise KeyError("Either IP or interface name should be provided")
        
        # TODO: linux output right now, can generalize for bsd with a couple additions
        
        #Because the interface information is found in multiple lines
        #flag indicates if gathering information about an interface is in progress
        flag = False
        
        for line in pipeIn('ifconfig'):
            # flag not True would indicate that we are looking for a new entry
            # if first character is line isalpha, then it is the beginning of a new interface entry
            if not flag and line[0].isalpha(): 
                # Start new entry
                (ip, name, mac, mask) = (None, None, None, None)
                p = line.split()
                name = p[0]
                if p[3] == 'HWaddr': 
                    mac = p[4]
                    
                # set flag
                flag = True

            elif flag and 'inet addr' in line:
                p = line.split()
                ip = p[1].split(':')[1]
                if 'Mask' in p[2]:
                    mask = p[2].split(':')[1]
                else:
                    mask = p[3].split(':')[1]
                    
                # all information should be complete by now
                # try to match with the required, and if successfully matched, return information
                if matchip and re.match(matchip, str(ip)): # see if we already had match on previous entry, uses re
                    return IFObj(ip, name, mac, mask)
                elif matchname and re.match(matchname, str(name)): # see if we already had match on previous entry, uses re
                    return IFObj(ip, name, mac, mask)
                
                # unset flag, look for another entry
                flag = False
                
        return IFObj(matchip, matchname, None, None)
    
    def getHostForName(self, name): return socket.gethostbyname(name)
    
    def getMulticastAddress(self):
        return helpers.getMulticast('project', 'experiment', 0)

