# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed, IFObj
from magi.util.execl import execAndRead, pipeIn
import itertools
import logging
import shlex
import socket
import sys


log = logging.getLogger(__name__)

class EmulabTestbed(Testbed):
    def __init__(self, **hint):
        Testbed.__init__(self)
        self._store = {}
        self.iflist = None
        self._ips = None
        self._nodes = None

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
        # the first node in the list  
        # The topomap is alphanumerically sorted on nodenames 
        completemap = self.getTopomap()
        host = completemap[1].split(',')[0]
        for lines in completemap:
            if 'control' == lines.split(',')[0].lower():
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

    def getLocalVirtualNodes(self):
        """ Get all the virtual nodes hosted by this machine """
        ret = list()
        for l in self.readAllLines(pipeIn('/usr/local/etc/emulab/tmcc vnodelist')):
            try:
                ret.append(self.parseVarLine(l)['VNODEID'])
            except:
                pass
        return ret

    def amAVirtualNode(self):
        """ return true if I am a virtual node (i.e. not a physical node or virtual host) """
        return len(execAndRead(["/usr/local/etc/emulab/tmcc", "jailconfig"])[0]) > 0

    def getInterfaceInfo(self, matchip=None, matchname=None):
        """ return IFObj for ip or name"""
        if not matchip and not matchname:
            raise KeyError("Either IP or interface name should be provided")
        if self.iflist is None:
            self.loadIfConfig()
        if matchip:
            for i in self.iflist:
                if i.ip == matchip:
                    return i
        for i in self.iflist:
            if i.name == matchname:
                return i
        raise KeyError("Invalid IP or interface name provided.")
    
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


    """ Abstracted 'readers' of data from 'locations' """

    def getSwapperData(self): return self.readFirstLine(pipeIn('/usr/local/etc/emulab/tmcc creator'))
    def getNicknameData(self): return self.readFirstLine(open('/var/emulab/boot/nickname', 'r'))
    def getControlIfData(self): return self.readFirstLine(open('/var/emulab/boot/controlif', 'r'))
    def getIfconfigData(self): return self.readAllLines(pipeIn('/usr/local/etc/emulab/tmcc ifconfig'))
    def getTopomap(self): return self.readAllLines(open('/var/emulab/boot/topomap'))

    def getTopoGraph(self):
        if 'topograph' not in self._store:
            self.loadTopoGraph()
        return self._store['topograph']
    
    def loadTopoGraph(self):
        import networkx as nx
        nodelist = False
        linkToNodeList = dict()
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
            linksinfo = []
            for link in links:
                linkname = link.split(":")[0]
                ip = link.split(":")[1]
                if linkname in linkToNodeList:
                    linkToNodeList[linkname].append(node)
                else:
                    linkToNodeList[linkname] = [node]
                    
                linksinfo.append({'name':linkname, 'ip':ip})
                    
            graph.add_node(node, links=linksinfo)
    
        for linkname in linkToNodeList.keys():
            nodeList = linkToNodeList[linkname]
            graph.add_edges_from(list(itertools.combinations(nodeList, 2)), linkname=linkname)
    
        self._store['topograph'] = graph


    def getIfFor(self, inet, mac):
        if (sys.platform == 'cygwin'):
            return execAndRead("ip2pcapif %s" % (inet))[0].strip()
        else:
            return execAndRead("/usr/local/etc/emulab/findif %s" % (mac))[0].strip()


    """ Functions that actually load the data into our _store """

    def loadEID(self):
        """ Load the nickname file to get the node, experiment and project names """
        try:
            self._store.update(node='?', experiment='?', project='?', eid='?', controlip='?')
            nickname = self.getNicknameData()
            p = nickname.split('.')
            self._store.update(node=p[0], experiment=p[1], project=p[2], eid=p[2]+"/"+p[1])
            self._store['controlip'] = self.getHostForName(nickname)
        except Exception, e:
            log.error("Can't load my hostname: %s" % e)


    def loadControlInfo(self):
        """ Load the control IP address and IF name files """
        try:
            self._store['controlif'] = None
            self._store['controlif'] = self.getControlIfData()
        except Exception, e:
            log.error("Can't load control IF: %s" % e)
        

    def loadIfConfig(self):
        """ Load all of the interface info from emulab/boot/tmcc/ifconfig """
        self.iflist = []

        # Split into lines, and parse the K=V pieces
        for line in self.getIfconfigData():
            args = self.parseVarLine(line)
            inet = args.get('INET', '')
            mask = args.get('MASK', '')
            # virtual nodes have no MAC, instead they have a VMAC
            mac = args.get('MAC', args.get('VMAC',''))
            name = self.getIfFor(inet, mac)
            if inet == '' or mac == '': continue
            self.iflist.append(IFObj(inet, name, mac, mask))


# Small test if running this file directly
if __name__ == "__main__":
    x = EmulabTestbed()
    print 'Control IP:', x.controlip
    print 'Node Name:', x.nodename
    print 'Control IF:', x.controlif
    
    iplist = x.getLocalIPList()
    print x.getServer()
    print 'Exp. Addresses: %s' % iplist
    print 'Exp. Interface info:'
    for ip in iplist:
        print '\t%s: %s' % (ip, x.getInterfaceInfo(ip))
