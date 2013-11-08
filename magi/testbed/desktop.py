# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from base import Testbed
import logging
import socket
import networkx as nx

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

        self._store['experiment'] = 'desktop'
        self._store['project'] = 'desktop'
        self._store['eid'] = 'desktop'


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
            self._store.update(node='?', experiment='?', project='?', eid='?', controlip='?')
            self._store['node'] = socket.gethostname()
        except Exception, e:
            log.error("Can't load my hostname: %s" % e)

    def getTopoGraph(self):
        if 'topograph' not in self._store:
            self.loadTopoGraph()
        return self._store['topograph']
    
    def loadTopoGraph(self):
        graph = nx.Graph()
        graph.add_node(self.getNodeName())
        self._store['topograph'] = graph

# Small test if running this file directly
if __name__ == "__main__":
    x = DesktopExperiment('tau')
    print 'Node Name:', x.nodename
    y = DesktopExperiment()
    print 'Node Name:', y.nodename

