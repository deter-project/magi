# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed
import logging
import socket

log = logging.getLogger(__name__)

class DesktopExperiment(Testbed):
    
    def __init__(self, node=None):
        Testbed.__init__(self)
        self._store = {}
        if node is None:
            self._store['node'] = socket.gethostname()
        else:
            self._store['node'] = node 

    def setNodeName(self, nodename):
        self._store['node'] = nodename
        
    def getExperimentDir(self):
        return "/tmp"

    def getFQDN(self):
        return self.nodename
    
    def getServer(self, FQDN=False):
        return Testbed.getServer(self, FQDN=False)
    
    """ Functions that actually load the data into our _store """

    def loadEID(self):
        """ Load the nickname file to get the node, experiment and project names """
        try:
            if not self._store['node']:
                self._store['node'] = socket.gethostname()
                
            self._store.update(experiment='desktopExperiment', 
                               project='desktopProject', 
                               eid='desktopExperiment/desktopProject')
        except Exception:
            log.exception("Can't load host information")

    def loadControlInfo(self):
        """ Load the control IP address and IF name """
        try:
            self._store.update(controlip='?', controlif='?')
            self._store['controlip'] = socket.gethostbyname(socket.gethostname())
            self._store['controlif'] = self.getInterfaceInfo(self.controlip).name
        except:
            log.exception("Can't load control interface info")
    
    def loadIfConfig(self):
        """ Load all of the interface info """
        try:
            iflist = []
            iflist.append(self.getIfconfigData(self.controlip))
            self._store['iflist'] = iflist
        except:
            log.exception("Can't load interface config data")
            
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

