# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed
import logging
import socket

log = logging.getLogger(__name__)

class DesktopExperiment(Testbed):
    
    def __init__(self, **hint):
        Testbed.__init__(self)
        self._store = {}
        if 'node' in hint:
            self._store['node'] = hint['node']

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
            if 'node' not in self._store:
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
    logging.basicConfig()
    x = DesktopExperiment()
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

    y = DesktopExperiment(node='tau')
    print 'Configured Node Name:', y.nodename

