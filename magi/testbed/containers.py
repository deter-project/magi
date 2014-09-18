
#!/usr/bin/env python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import IFObj
from collections import defaultdict
from emulab import EmulabTestbed
from magi.util.execl import pipeIn
import itertools
import logging
import re
import socket
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)

class ContainerExperiment(EmulabTestbed):
    
    def __init__(self):
        EmulabTestbed.__init__(self)
        self._store = {}
        self._confdir = '/var/containers/config/'

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
                    
            self._store['controlif'] = self.getIfconfigData(self.controlip).name
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
    
    def getIfconfigData(self, matchip=None, matchname=None):
        """ Get the name and MAC address for an interface given its IP address, IP can be a regular expression """
        if not matchip and not matchname:
            raise KeyError("Either IP or interface name should be provided")
        
        (ip, name, mac, mask) = (None, None, None, None)

        # TODO: linux output right now, can generalize for bsd with a couple additions
        for line in pipeIn('ifconfig'):
            # new interface name entry
            if line[0].isalpha():  
                if matchip and re.match(matchip, str(ip)): # see if we already had match on previous entry, uses re
                    return IFObj(ip, name, mac, mask)
                elif matchname and re.match(matchname, str(name)): # see if we already had match on previous entry, uses re
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
    
# Small test if running this file directly
if __name__ == "__main__":
    logging.basicConfig()
    x = ContainerExperiment()
    print 'Node Name:', x.nodename
    print 'FQDN:', x.fqdn
    print 'Control IP:', x.controlip
    print 'Control IF:', x.controlif
    print 'Server Node:', x.getServer()
    
    iplist = x.getLocalIPList()
    print 'Exp. Addresses: %s' % iplist
    print 'Exp. Interface info:'
    for ip in iplist:
        print '\t%s: %s' % (ip, x.getInterfaceInfo(ip))

