import logging
import string
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

class ControlGraph(object):
    """ Create the call graph from the procedure description 
    """
    def __init__(self):
        ''' Initialize a control graph for display '''
        self.controlgraph = dict()
        
        # Sub graph/cluster keys
        self.keys = list()
        # Inter-cluster edges
        self.graphEdges = list()
        # Set of global  outgoing events/triggers mapped to corresponding nodes
        self.graphOutgoing = defaultdict(set)
        # Set of global outgoing events/triggers mapped to corresponding nodes
        self.graphIncoming = defaultdict(set)

        self.defaultClusters = ['setup', 'exit', 'env']
        # Currently setup and exit nodes are represented at singleton events 
        for label in self.defaultClusters:
            self.addCompositeCluster(label)

        # List of active/visible default clusters
        self.activeDefaultClusters= list()
        # Always show the setup cluster
        self.activeDefaultClusters.append('setup')

    def createCluster(self, key):
        '''
            Create a cluster of connected nodes
            Each event stream is represented by a cluster
        '''
        self.keys.append(key)
        self.controlgraph[key] = ControlGraphCluster(key) 
        return self.controlgraph[key]

    def addCompositeCluster(self, key):
        '''
            Create a singleton cluster
        '''
        self.createCluster(key)
        self.controlgraph[key].addLabel(label=string.upper(key))  

    def addEvent(self, key, event):
        '''
            Add event to the cluster corresponding to the key
        '''
        self.controlgraph[key].addEvent(event)

    def addTrigger(self, key, event):
        '''
            Add trigger to the cluster corresponding to the key
        '''
        self.controlgraph[key].addTrigger(event)

    def addEdges(self, key):
        '''
            Create edges for the cluster corresponding to the key
        '''
        self.controlgraph[key].addEdges()
    
    def finishCluster(self, key):
        self.addEdges(key)
        
    def finishControlgraph(self):
        
        # Add edge from setup to the start of each cluster
        for k in self.keys:
            # Skip default clusters
            if k in self.defaultClusters:
                continue
            
            self.addEdge(self.controlgraph['setup'].nodes[0]['id'], 
                         self.controlgraph[k].getFirstValidNode()['id'],
                         self.getEdgeIndex())
            
        # Merge event/trigger based edges from all the clusters
        for k in self.keys:
            # Skip default clusters
            if k in self.defaultClusters:
                continue
            
            for event, nodes in self.controlgraph[k].incoming.iteritems():
                self.graphIncoming[event].update(nodes)
                
            for event, nodes in self.controlgraph[k].outgoing.iteritems():
                self.graphOutgoing[event].update(nodes)
            
        log.debug("Outgoing events: %s" %(self.graphOutgoing))
        log.debug("Incoming events: %s" %(self.graphIncoming))
        
        # If an edge to exit exists, activate exit cluster
        if 'exit' in self.graphOutgoing:
            self.activeDefaultClusters.append('exit')
        
        # Add edges
        log.debug("Adding global edges")
        for event in self.graphOutgoing:
            log.debug("Outgoing Event: %s" %(event))
            if event in self.graphIncoming:
                log.debug("Corresponding incoming event exists")
                # Found a set of nodes that are waiting for this event 
                # Now create an edge for each pair of outgoing and incoming node
                # corresponding to the event
                for fromNode in self.graphOutgoing[event]: 
                    for toNode in self.graphIncoming[event]:
                            self.addEdge(fromNode,
                                         toNode,
                                         self.getEdgeIndex(),
                                         event)
                
                # Done with this incoming event
                del self.graphIncoming[event]

            else:
                # check is there is a stream by that name 
                log.debug("Corresponding incoming event does not exist")
                log.debug("Checking if the event is a cluster key")
                if event in self.keys:
                    log.debug("Event is a valid cluster key")
                    log.debug("Adding edges to the cluster")
                    # From each node corresponding to the outgoing event
                    # add an edge to the start of the cluster
                    for fromNode in self.graphOutgoing[event]:
                        self.addEdge(fromNode, 
                                     self.controlgraph[event].nodes[0]['id'],
                                     self.getEdgeIndex(),
                                     "Jump")
                else:
                    log.debug("Event is not a valid cluster key")
                    for fromNode in self.graphOutgoing[event]:
                        if "Trigger" in fromNode:
                            # No target by that name. jumping to some place in the environment
                            log.critical("Trying to jump to target %s, but it does not exist", event)
                        else:
                            # Did not find a set of nodes waiting for the event generated by this node. :( 
                            # send the event to the environment
                            # Activate the environment cluster
                            self.activeDefaultClusters.append('env')
                            self.addEdge(fromNode,
                                         self.controlgraph['env'].nodes[0]['id'],
                                         self.getEdgeIndex(),
                                         event)

        if len(self.graphIncoming) > 0:
            # Did not find a set of nodes generating the event that is being waited for by some nodes.
            # These are events coming in from the environment
            # Activate the environment cluster
            self.activeDefaultClusters.append('env')
            edgeId = 0
            for event in self.graphIncoming:
                for toNode in self.graphIncoming[event]:
                    self.addEdge(self.controlgraph['env'].nodes[0]['id'],
                                 toNode,
                                 edgeId,
                                 event)
                    edgeId += 1 
            
    def addEdge(self, fromNode, toNode, edgeId, label=" "):
        newEdge = dict()
        newEdge['id'] = 'Edge' + str(edgeId) 
        newEdge['type'] = 'edge'
        newEdge['label'] = label 
        newEdge['from'] = fromNode
        newEdge['to'] = toNode 
        self.graphEdges.append(newEdge)

    def getEdgeIndex(self):
        return len(self.graphEdges)
    
    
    def createDot(self):
        import pydot 
        # Walk through all the control graph clusters.
        # For the default clusters, add only the active clusters
        # For all the other clusters, add them
        # Create all the required edges
        dotObj = pydot.Dot(graph_type='digraph', fontname="Verdana")
        for k in self.keys:
            if k in self.defaultClusters:
                if k in self.activeDefaultClusters:
                    label = self.controlgraph[k].nodes[0]['label']
                    fillcolor = 'steelblue'
                    if label == 'EXIT':
                        fillcolor = 'red'
                    dotObj.add_node(pydot.Node(self.controlgraph[k].nodes[0]['id'], 
                                               label = self.controlgraph[k].nodes[0]['label'], 
                                               shape='box', style='filled, rounded', 
                                               fillcolor=fillcolor))
            else:
                c = pydot.Cluster(k, label=' ')

                allnodes = self.controlgraph[k].getAllNodes()
                for n in allnodes:
                    if n['type'] != 'syncnode':
                        if n['type'] == 'event':
                            fillcolor = 'lightblue'
                        elif n['type'] == 'trigger':
                            fillcolor = 'orange'
                        else:
                            fillcolor = 'steelblue'
                            
                        c.add_node(pydot.Node(n['id'], label=n['label'], shape='box', style='filled, rounded', fillcolor=fillcolor))

                dotObj.add_subgraph(c)

                clusterEdges = self.controlgraph[k].getAllEdges()
                for edge in clusterEdges:
                    dotObj.add_edge(pydot.Edge(edge['from'], 
                                             edge['to'], 
                                             label=edge['label']))
                    
        for edge in self.graphEdges:
            dotObj.add_edge(pydot.Edge(edge['from'], 
                                     edge['to'], 
                                     label=edge['label']))

        return dotObj
    
    def writeRaw(self, fileName='test.dot'):
        self.createDot().write_raw(fileName)
            
    def writePng(self, fileName='test.png'):
        self.createDot().write_png(fileName)
        
    def createSvg(self):
        return self.createDot().create_svg()
        
    def __repr__(self):
        rstr = "Control Graph\n" 
        for k, v in self.controlgraph.items():
            rstr = rstr + str(k) + "\n" + v.__repr__()+ "\n"
        return rstr 

class ControlGraphCluster(dict):
    """ Creates a cluster of connected control graph nodes  
    """

    def __init__(self, key):
        # Cluster name
        self.key = key
        # Cluster nodes
        self.nodes = list()
        # Cluster internal edges
        self.edges = list()
        # Set of outgoing events/triggers mapped to corresponding nodes
        self.outgoing = defaultdict(set) 
        # Set of incoming events/triggers mapped to corresponding nodes
        self.incoming = defaultdict(set)
        
    def addEvent(self, event):
        log.debug("Adding Event : %s" %(event)) 
        newNode = dict()
        newNode['type'] = 'event'
        newNode['id']= "%s%dEvent" %(str(self.key), self.getNodeIndex())
        newNode['edgeto']= set()
        label = "Send" 
        if event.trigger:
            newNode['edgeto'].add(event.trigger)
            # outgoing trigger from this node
            self.outgoing[event.trigger].add(newNode['id'])
        #label += "\nAgent: %s" %(event.agent)
        #label += "\nMethod: %s" %(event.method)
        label += " %s -> %s" %(event.agent, event.method)
        newNode['label'] = label 
        self.nodes.append(newNode)

    def addTrigger(self, triggerList):
        import parse
        log.debug("Adding Trigger List : %s" %(triggerList)) 
        # A trigger list is a list of triggers
        # We parse each trigger and the first trigger to complete
        # decides the control path 
        
        eventTriggers = parse.getEventTriggers(triggerList)

        self.nodes.append({'type' : 'syncnode', 'id' : 'StartTriggerList'})
        
        for trigger in triggerList:
            newNode = dict()
            newNode['type'] = 'trigger' 
            newNode['id'] = "%s%dTrigger" %(str(self.key), self.getNodeIndex())
            newNode['edgefrom']=set()
            newNode['edgeto']=set()
            
            eventTriggers = parse.getEventTriggers(trigger)
            
            for eventTrigger in eventTriggers:
                newNode['edgefrom'].add(eventTrigger.event)
                # incoming trigger to this node
                self.incoming[eventTrigger.event].add(newNode['id'])
            
            if trigger.target:
            # These targets are eventstreams 
            # Hence we can directly add a edge here 
                newNode['edgeto'].add(trigger.target)
                # outgoing to target from this node
                self.outgoing[trigger.target].add(newNode['id'])
            
            label = "Wait for %s" %(trigger.toString())
            if parse.getTriggerType(trigger) == parse.Trigger.EVENT:
                if trigger.count != 1:
                    label = label + "\n" + "Count: " + str(trigger.count) 
            
            newNode['label'] = label 
            self.nodes.append(newNode)

        self.nodes.append({'type' : 'syncnode', 'id' : 'EndTriggerList'})
                
    def addLabel(self, label):
        log.debug("Adding Label : %s" %(label)) 
        newNode = dict()
        newNode['type'] = 'node'
        newNode['id']= "%s%dLabel" %(str(self.key), self.getNodeIndex())
        newNode['edgeto']= set()
        newNode['label'] = label 
        self.nodes.append(newNode)
        
    def addEdges(self):
        '''
            Add edges between nodes within the cluster
        '''
        prevNodes = list()
        currentNodes = list()
        
        nodesItr = iter(self.nodes)
        
        try:

            while True:
                node = next(nodesItr)
                log.debug("Node Type: %s" %(node['type']))
                log.debug("Node ID: %s" %(node['id']))
                
                hasTarget = list()
                
                if node['type'] in ['event', 'trigger', 'node']:
                    # It has to be an event, as it is outside the trigger list markers
                    currentNodes.append(node['id'])
                
                elif node['type'] == 'syncnode':
                    node = next(nodesItr)
                    log.debug("Node Type: %s" %(node['type']))
                    log.debug("Node ID: %s" %(node['id']))
                    
                    while node['type'] != 'syncnode':
                        currentNodes.append(node['id'])
                        if len(node['edgeto']) != 0:
                            hasTarget.append(node['id'])
                        node = next(nodesItr)
                        log.debug("Node Type: %s" %(node['type']))
                        log.debug("Node ID: %s" %(node['id']))
                else:
                    log.exception('Invalid Node Type')
                    raise TypeError('Invalid Node Type')
                        
                log.debug("Previous Nodes: %s" %(prevNodes))
                log.debug("Current Nodes: %s" %(currentNodes))
                log.debug("Trigger Nodes with target: %s" %(hasTarget))
            
                # This code does not have the data you need 
                for pnode in prevNodes:
                    for cnode in currentNodes:
                        self.addEdge(pnode, cnode)
    
                # Create the previous nodes list based on 
                # current nodes that do not have a target 
                prevNodes = list()
                for cnode in currentNodes:
                    if cnode not in hasTarget:
                        prevNodes.append(cnode)
                        
                currentNodes = list() 
                
        except StopIteration:
            log.debug("Done with all edges")

    def addEdge(self, fromNode, toNode, label=" "):
        '''
            Add an edge between nodes within the cluster
        '''
        newEdge = dict()
        newEdge['id'] = "%sEdge%d" %(str(self.key), self.getEdgeIndex())
        newEdge['type'] = 'edge'
        newEdge['label'] = label 
        newEdge['from'] = fromNode
        newEdge['to'] = toNode 
        self.edges.append(newEdge)

    def getNodeIndex(self):
        return len(self.nodes)

    def getEdgeIndex(self):
        return len(self.edges)
    
    def getAllNodes(self):
        return self.nodes 

    def getAllEdges(self):
        return self.edges
    
    def getFirstValidNode(self):
        ''' Fetch the first non-syncnode '''
        for node in self.nodes:
            if node['type'] != 'syncnode':
                return node
        raise Exception("No valid node found")
    
    def __repr__(self):
        rstr = "Cluster Graph: " + self.key + "\n"
        for n in self.nodes:
            for k,v in n.items():
                rstr = rstr +  " " + str(k) + ':' + str(v) +"\n" 
            rstr += "\n"
        for e in self.edges:
            for k,v in e.items():
                rstr = rstr +  " " + str(k) + ':' + str(v) +"\n" 
            rstr += "\n"
        return rstr 


