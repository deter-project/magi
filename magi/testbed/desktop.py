# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed, IFObj
import logging
import socket

log = logging.getLogger(__name__)

class DesktopExperiment(Testbed):
    
    def __init__(self, node=None):
        Testbed.__init__(self)
        self._store = {}
        self.iflist = None
        self._ips = None
        self._nodes = None

        if node is None:
            self._store['node'] = socket.gethostname()
        else:
            self._store['node'] = node 


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

    def getExperimentDir(self):
        return "/tmp"
    
    def getControlIP(self):
        if 'controlip' not in self._store:
            self.loadEID()
        return self._store['controlip']

    def getControlIF(self):
        if 'controlif' not in self._store:
            self.loadControlInfo()
        return self._store['controlif']

    def getLocalIPList(self):
        """returns list of IP """
        return [self.getControlIP()]

    """ Local node properties (readonly) """        
    def getNodeName(self):
        if 'node' not in self._store:
            self.loadEID()
        return self._store['node']
    
    def setNodeName(self, nodename):
        self._store['node'] = nodename

    """ Functions that actually load the data into our _store """

    def loadEID(self):
        """ Load the nickname file to get the node, experiment and project names """
        try:
            if not self._store['node']:
                self._store['node'] = socket.gethostname()
                
            self._store.update(experiment='desktopExperiment', 
                               project='desktopProject', 
                               eid='desktopExperiment/desktopProject', 
                               controlip=socket.gethostbyname(socket.gethostname()))
        except Exception:
            log.exception("Can't load host information")

    def loadControlInfo(self):
        """ Load the control IP address and IF name files """
        try:
            info = self.getInterfaceInfo(self.getControlIP())
            self._store['controlif'] = info.name
        except Exception, e:
            log.error("Can't load control IF: %s", e)

#    def loadIfConfig(self):
#        """ Load all of the interface info """
#        self.iflist = []
#        self.iflist.append(IFObj(name='eth1', ip='10.0.0.1', mask='255.255.255.0', mac='00:00:00:00:00:01'))

    def getTopoGraph(self):
        if 'topograph' not in self._store:
            self.loadTopoGraph()
        return self._store['topograph']
    
    def loadTopoGraph(self):
        import networkx as nx
        graph = nx.Graph()
        graph.add_node(self.getNodeName(), links={})
        self._store['topograph'] = graph

# Small test if running this file directly
if __name__ == "__main__":
    x = DesktopExperiment('tau')
    print 'Node Name:', x.nodename
    y = DesktopExperiment()
    print 'Node Name:', y.nodename

