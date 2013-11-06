# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import re

from magi.util.execl import pipeIn

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
    def experiment(self):
        """ the experiment name """
        return self.getExperiment()

    @property
    def project(self):
        """ the project name """
        return self.getProject()

    @property
    def eid(self):
        """ the experiment 'id' string """
        return self.getExperimentID()


    @property
    def nodename(self):
        """ local node name """
        return self.getNodeName()

    @property
    def controlip(self):
        """ local control interface IP address """
        return self.getControlIP()

    @property
    def controlif(self):
        """ local control interface name """
        return self.getControlIF()


    """ Queries for this Node """

    def getLocalIPList(self):
        """ returns list of IP addresses for this node """
        NIE()

    def getLocalIFList(self):
        """ returns list of interface names for this node """
        NIE()

    def getInterfaceList(self):
        """ returns a list of IFObj's """
        NIE()

    def getInterfaceInfo(self, ip):
        """ returns a single IFObj for ip """
        NIE()

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

    def getInterfaceInfo(self, matchip):
        """ Get the name and MAC address for an interface given its IP address, IP can be a regular expression """
        (ip, name, mac, mask) = (None, None, None, None)

        # TODO: linux output right now, can generalize for bsd with a couple additions
        for line in pipeIn('ifconfig'):
            # new interface name entry
            if line[0].isalpha():  
                if  re.match(matchip, str(ip)): # see if we already had match on previous entry, uses re
                    return IFObj(ip, name, mac, mask)
            
                # Otherwise start new entry
                (ip, name, mac, mask) = (None, None, None, None)
                p = line.split()
                name = p[0]
                if p[3] == 'HWaddr': 
                    mac = p[4]

            elif 'inet addr' in line:
                p = line.split()
                ip = p[1].split(':')[1]
                if 'Mask' in p[2]:
                    mask = p[2].split(':')[1]
                else:
                    mask = p[3].split(':')[1]

        return IFObj(matchip, None, None, None)

