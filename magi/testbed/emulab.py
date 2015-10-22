# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from collections import defaultdict
import itertools
import logging
import os
import shlex
import sys

from magi.util import helpers
from magi.util.execl import execAndRead, pipeIn

from base import Testbed, IFObj


log = logging.getLogger(__name__)

class EmulabTestbed(Testbed):
    
    def __init__(self):
        Testbed.__init__(self)
        self._store = {}
    
    """    Testbed Properties (readonly) """
    
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
    
    def getExperimentDir(self):
        return os.path.join('/proj', self.getProject(), 'exp', self.getExperiment())

    def toControlPlaneNodeName(self, nodename):
        if nodename not in ['localhost', '127.0.0.1'] and '.' not in nodename:
            nodename += '.%s.%s' % (self.getExperiment(), self.getProject())
        return nodename

    """ Queries for this Node """
    def getLocalVirtualNodes(self):
        """ Get all the virtual nodes hosted by this machine """
        ret = list()
        for l in self.readAllLines(pipeIn('/usr/local/etc/emulab/tmcc vnodelist')):
            try:
                ret.append(self.parseVarLine(l)['VNODEID'])
            except:
                pass
        return ret

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
    
    def amAVirtualNode(self):
        """ return true if I am a virtual node (i.e. not a physical node or virtual host) """
        return len(execAndRead(["/usr/local/etc/emulab/tmcc", "jailconfig"])[0]) > 0


    """ Functions that actually load the data into our _store """

    def loadEID(self):
        """ Load the nickname file to get the node, experiment and project names """
        try:
            self._store.update(node='?', experiment='?', project='?', eid='?')
            nickname = self.getNicknameData()
            p = nickname.split('.')
            self._store.update(node=p[0], experiment=p[1], project=p[2], 
                               eid=p[2]+"/"+p[1])
        except:
            log.exception("Can't load my host info")
            
    def setEID(self, node=None, experiment=None, project=None):
        """ Set the node, experiment, and project name """
        if node:
            self._store.update(node=node)
        if experiment:
            self._store.update(experiment=experiment)
        if project:
            self._store.update(project=project)   
             
        self._store.update(eid=self.project+"/"+self.experiment)

    def loadControlInfo(self):
        """ Load the control IP address and IF name files """
        try:
            self._store.update(controlip='?', controlif='?')
            nickname = self.getNicknameData()
            self._store['controlip'] = self.getHostForName(nickname)
            self._store['controlif'] = self.getControlIfData()
        except:
            log.exception("Can't load control interface info")
    
    def loadIfConfig(self):
        """ Load all of the interface info from emulab/boot/tmcc/ifconfig """
        try:
            iflist = []
            # Split into lines, and parse the K=V pieces
            for line in self.getIfconfigData():
                args = self.parseVarLine(line)
                inet = args.get('INET', '')
                mask = args.get('MASK', '')
                # virtual nodes have no MAC, instead they have a VMAC
                mac = args.get('MAC', args.get('VMAC',''))
                name = self.getIfFor(inet, mac)
                if inet == '' or mac == '': continue
                iflist.append(IFObj(inet, name, mac, mask))
    
            self._store['iflist'] = iflist
        except:
            log.exception("Can't load interface config data")
        
    def loadTopoGraph(self):
        try:
            import networkx as nx
            nodelist = False
            linkToNodeList = defaultdict(set)
            graph = nx.Graph()
        
            for e in self.getTopomap():
                if not nodelist:
                    if "# nodes" in e:
                        nodelist = True
                        continue
                if "# lans" in e:
                    break
            
                node = e.split(",")[0]
                links = e.split(",")[1].split()
                linksInfo = dict()
                for link in links:
                    linkName = link.split(":")[0]
                    ip = link.split(":")[1]
                    linkToNodeList[linkName].add(node)
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


    """ Abstracted 'readers' of data from 'locations' """

    def getSwapperData(self): return self.readFirstLine(pipeIn('/usr/local/etc/emulab/tmcc creator'))
    def getNicknameData(self): return self.readFirstLine(open('/var/emulab/boot/nickname', 'r'))
    def getControlIfData(self): return self.readFirstLine(open('/var/emulab/boot/controlif', 'r'))
    def getIfconfigData(self): return self.readAllLines(pipeIn('/usr/local/etc/emulab/tmcc ifconfig'))
    def getTopomap(self): return self.readAllLines(open('/var/emulab/boot/topomap'))

    def getIfFor(self, inet, mac):
        if (sys.platform == 'cygwin'):
            return execAndRead("ip2pcapif %s" % (inet))[0].strip()
        else:
            return execAndRead("/usr/local/etc/emulab/findif %s" % (mac))[0].strip()            
        
    def getMulticastAddress(self):
        return helpers.getMulticast(self.project, self.experiment, 0)
    
# Small test if running this file directly
if __name__ == "__main__":
    logging.basicConfig()
    x = EmulabTestbed()
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
