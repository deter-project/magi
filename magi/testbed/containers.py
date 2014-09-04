#!/usr/bin/env python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed
from collections import defaultdict
import itertools
import logging
import os
import re
import socket
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)

class ContainerExperiment(Testbed):
    
    def __init__(self, **hint):
        Testbed.__init__(self)
        self._store = {}
        self._confdir = '/var/containers/config/'
        if 'project' in hint and 'experiment' in hint:
            self._store.update(project=hint['project'], experiment=['experiment'], 
                                eid=hint['project']+"/"+hint['experiment'],
                                node='none', controlip='none')

    def getExperimentDir(self):
        return os.path.join('/proj', self.getProject(), 'exp', self.getExperiment())
    
    """ Queries for this Node """
    
    def amAVirtualNode(self):
        """ return true if I am a virtual node (i.e. not a physical node or virtual host) """
        return True
    
    """ Functions that actually load the data into our _store """

    def loadEID(self):
        """ Load the nickname file to get the node, experiment and project names """
        try:
            self._store.update(node='?', experiment='?', project='?', eid='?')
            
            # Get pid and eid from file /var/containers/pid_eid
            fp = open(self._confdir+'pid_eid', 'r')
            proj = fp.readline().strip()
            exp = fp.readline().strip()
            fp.close()

            host = socket.gethostname()
            if '.' in host:
                host = host.split('.')[0]

            self._store.update(node=host, experiment=exp, project=proj, eid=proj+"/"+exp)
        except:
            log.error("Can't load my host info")

    def loadControlInfo(self):
        """ Load the control IP address and IF name """
        try:
            self._store.update(controlip='?', controlif='?')
            try:
                self._store['controlip'] = socket.gethostbyname("%s.%s.%s" % (self.nodename, self.experiment, self.project))
            except:
                log.error("Couldn't get my hostname, searching for 192.168 interfaces")
                ifobj = self.getInterfaceInfo("192.168.*")
                if ifobj.name is not None:  # found a match
                    self._store['controlip'] = ifobj.ip
                    
            self._store['controlif'] = self.getInterfaceInfo(self.controlip).name
        except:
            log.exception("Can't load control interface info")
        
    def loadIfConfig(self):
        """ Load all of the interface info from /var/containers/config/hosts """
        try:
            iflist = []
            exp = re.compile('( %s-[0-9]+)' % self.nodename)
            fp = open(self._confdir+'hosts')
            for line in fp:
                match = exp.search(line)
                if match is None: continue
                iflist.append(self.getIfconfigData(line.split()[0]))
            fp.close()
            self._store['iflist'] = iflist
        except:
            log.exception("Can't load interface config data")

    def loadTopoGraph(self):
        try:
            import networkx as nx
            linkToNodeList = defaultdict(set)
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
                linksInfo = dict()
                for interface in computer.findall('interface'):
                    linkName = interface.find('substrate').text
                    if linkName == 'control_net': continue
                    linkToNodeList[linkName].add(node)
                        
                    for attribute in interface.findall('attribute'):
                        if attribute.find('attribute').text == 'ip4_address':
                            ip = attribute.find('value').text
                            break
                        
                    linksInfo[linkName] = {'name':linkName, 'ip':ip}
                        
                graph.add_node(node, links=linksInfo)
            
            for linkName in linkToNodeList.keys():
                nodeSet = linkToNodeList[linkName]
                for node in nodeSet:
                    graph.node[node]['links'][linkName]['peerNodes'] = list(nodeSet - set([node]))
                graph.add_edges_from(list(itertools.combinations(nodeSet, 2)), linkName=linkName)
                
            self._store['topograph'] = graph
        except:
            log.exception("Can't load topology graph")

    def getTopoXml(self): return self.readAllLines(open(self._confdir+'topo.xml'))
    def getPhysTopoXml(self): return self.readAllLines(open(self._confdir+'phys_topo.xml'))
    
# Small test if running this file directly
if __name__ == "__main__":
    logging.basicConfig()
    x = ContainerExperiment()
    print x.controlip
    print x.nodename
    print x.controlif
    print x.getInterfaceList()
    print x._store

