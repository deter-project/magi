#!/usr/bin/env python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed
from collections import deque
import itertools
import logging
import re
import shlex
import socket
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)

class ContainerExperiment(Testbed):
    def __init__(self, **hint):
        Testbed.__init__(self)
        self._store = {}
        self.iflist = None
        self._ips = None
        self._nodes = None
        self._confdir = '/var/containers/config/'

        if 'project' in hint and 'experiment' in hint:
            self._store.update(project=hint['project'], experiment=['experiment'], 
                                eid=hint['project']+"/"+hint['experiment'],
                                node='none', controlip='none')

    """    Testbed Properties (readonly) """
    def getExperiment(self):
        if 'experiment' not in self._store:
            self.loadEID()
        return self._store['experiment']

    def getProject(self):
        if 'project' not in self._store:
            self.loadEID()
        return self._store['project']

    def getExperimentID(self):
        if 'eid' not in self._store:
            self.loadEID()
        return self._store['eid']
    
    def getServer(self, FQDN=False):
        # Gets the complete topology map and returns control if 
        # if it finds a node named "control" otherwise it returns 
        # the first node in the alpha-numerically sorted list  
        topoGraph = self.getTopoGraph()
        nodes = topoGraph.nodes()
        nodes.sort()
        host = nodes[0]
        for node in nodes:
            if 'control' == node.lower():
                host = 'control'
                break
        
        if FQDN:
            return '%s.%s.%s' % (host, self.getExperiment(), self.getProject())
        else:
            return host
        
    """ Local node properties (readonly) """
    def getNodeName(self):
        if 'node' not in self._store:
            self.loadEID()
        return self._store['node']

    def getControlIP(self):
        if 'controlip' not in self._store:
            self.loadEID()
        return self._store['controlip']

    def getControlIF(self):
        if 'controlif' not in self._store:
            self.loadControlInfo()
        return self._store['controlif']
        
    """ Queries for this Node """
    def getLocalIPList(self):
        """returns list of IP """
        if self.iflist is None:
            self.loadIfConfig()
        return [obj.ip for obj in self.iflist]

    def getLocalIFList(self):
        """returns list of IF """
        if self.iflist is None:
            self.loadIfConfig()
        return [obj.name for obj in self.iflist]

    def getInterfaceList(self):
        """ return the list of IFObj's """
        if self.iflist is None:
            self.loadIfConfig()
        return self.iflist

    def amAVirtualNode(self):
        """ return true if I am a virtual node (i.e. not a physical node or virtual host) """
        return True
    
    def parseVarLine(self, line):
        args = {}
        for x in shlex.split(line):
            sp = x.split('=')
            if sp[0] == '':
                continue
            if (len(sp) == 1):
                args[sp[0]] = '1'
            else:
                args[sp[0]] = sp[1]
        return args


    """ Functions that actually load the data into our _store """

    def loadEID(self):
        """ Load the nickname file to get the node, experiment and project names """
        try:
            self._store.update(node='?', experiment='?', project='?', eid='?', controlip='?')
            # Get pid from file /var/containers/pid 
            fp = open(self._confdir+'pid_eid', 'r')
            proj = fp.readline().strip()

            # Get eid from file /var/containers/eid 
            exp = fp.readline().strip()
            fp.close()

            host = socket.gethostname()
            if '.' in host:
                host = host.split('.')[0]

            self._store.update(node=host, experiment=exp, project=proj, eid=proj+"/"+exp)
            try:
                self._store['controlip'] = socket.gethostbyname("%s.%s.%s" % (host, exp, proj))
            except Exception, e:
                log.error("Couldn't get my hostname, searching for 192.168 interfaces")
                ifobj = self.getInterfaceInfo("192.168.*")
                if ifobj.name is not None:  # found a match
                    self._store['controlip'] = ifobj.ip
            
        except Exception, e:
            log.error("Can't load my host info: %s", e)


    def loadControlInfo(self):
        """ Load the control IP address and IF name files """
        try:
            info = self.getInterfaceInfo(self.controlip)
            self._store['controlif'] = info.name
        except Exception, e:
            log.error("Can't load control IF: %s", e)
        

    def loadIfConfig(self):
        """ Load all of the interface info from emulab/boot/tmcc/ifconfig """
        self.iflist = []

        try:
            exp = re.compile('( %s-[0-9]+)' % self.nodename)
            fp = open(self._confdir+'hosts')
            for line in fp:
                match = exp.search(line)
                if match is None: continue
                self.iflist.append(self.getInterfaceInfo(line.split()[0]))
            fp.close()
        except Exception, e:
            log.error("Can't load interface list: %s", e)


    def getTopoXml(self): return self.readAllLines(open(self._confdir+'topo.xml'))
    def getPhysTopoXml(self): return self.readAllLines(open(self._confdir+'phys_topo.xml'))
    
    def getTopoGraph(self):
        if 'topograph' not in self._store:
            self.loadTopoGraph()
        return self._store['topograph']
    
    def loadTopoGraph(self):
        import networkx as nx
        linkToNodeList = dict()
        graph = nx.Graph()
        root = ET.fromstring(self.getTopoXml()[0])
        
        for element in root.findall('elements'):
            
            pnode = False
            computer = element.find('computer')
            
            for attribute in computer.findall('attribute'):
                if attribute.find('attribute').text == 'containers:node_type':
                    if attribute.find('value').text == 'pnode':
                        pnode = True
                    break
            
            if pnode:
                continue

            node = computer.find('name').text
            linksinfo = []
            for interface in computer.findall('interface'):
                linkname = interface.find('substrate').text
                if linkname == 'control_net': continue
                if linkname in linkToNodeList:
                    linkToNodeList[linkname].append(node)
                else:
                    linkToNodeList[linkname] = [node]
                    
                for attribute in interface.findall('attribute'):
                    if attribute.find('attribute').text == 'ip4_address':
                        ip = attribute.find('value').text
                        break
                    
                linksinfo.append({'name':linkname, 'ip':ip})
                    
            graph.add_node(node, links=linksinfo)
        
        for linkname in linkToNodeList.keys():
            nodeList = linkToNodeList[linkname]
            graph.add_edges_from(list(itertools.combinations(nodeList, 2)), linkname=linkname)
            
        self._store['topograph'] = graph


# Small test if running this file directly
if __name__ == "__main__":
    logging.basicConfig()
    x = ContainerExperiment()
    print x.controlip
    print x.nodename
    print x.controlif
    print x.getInterfaceList()
    print x._store

