import logging
import yaml
import sys
import cStringIO
import optparse
from collections import defaultdict
from magi.messaging.api import MAGIMessage
import pdb


log = logging.getLogger(__name__)

"""
    sample AAL:
    -----------
    streamstarts: [ phase1 ]

    groups: {
        name: &reference {}
        name: {}
    }

    eventstreams: {
        phase1: [ E, E, T, E, T, ST ]
        phase2: [ E, E, T, E, T, ST ]
    }

    agents: {
        name: {}
        name: []
    }


    { type: event, ....}
    { type: trigger, triggers: [{timeout:1, event:X, otherkey:Y, target:null},
        ...] }

"""


class AALParseError(Exception):
    '''Small wrapper around exception for AAL parse errors.'''
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return repr(self.error)


class TriggerData(object):
    """
    Wrapper for incoming trigger data that splits key/scalar from
    key/lists
    """
    def __init__(self, datadict):
        self.args = dict()
        self.sets = defaultdict(set)
        self.count = [1, 0]  # default is always to look for a single match
        self.selfDestructTime = -1.0

        # handle 'meta' constraint data
        if 'count' in datadict:
            self.count = [int(datadict['count']), 0]
            del datadict['count']
        for k, v in datadict.iteritems():
            log.debug("unpacking incoming trigger k: %s, v: %s", k,v)
            if isinstance(v, list):
                self.sets[k] = set(v)
            elif isinstance(v, dict):
                if k == 'retVal':
                    log.debug("received a dict as return value from agent")
                    for kk, vv in v.items():
                        self.args[kk]= vv 
                else:
                    log.critical("received a dict on non retVal key, Do not know what to do")
            else:
                if (k == 'retVal') and (v == True) is True: 
                    continue
                self.args[k] = v
        log.debug("datadict: %s", datadict)
        log.debug("sets: %s", self.sets) 
        log.debug("args: %s", self.args)
        log.debug("count: %s", self.count)

    def update(self, incomingTrigData):
        '''
            Update trigger constraints. Should only be called with
            matching data.
        '''
        log.debug("Updating count for trigger %s", incomingTrigData)
        incomingTrigData.count[1] += 1
        log.debug("Count needed is %s current is %s", self.count[1], incomingTrigData.count[1]) 

    def reset(self):
        log.debug("reseting the count for trigger")
        self.count[1] = 0 


    def constraintMatched(self):
        '''Returns True if meta trigger data matches. '''
        if self.count[0] and self.count[0] == self.count[1]:
            # If the current time is passed this value, the trigger
            # can be deleted.
            return True
        return False

    def shouldDelete(self, curTime):
        if self.selfDestructTime is None:
            return False   # we don't care about self destruct
        else:
            return curTime > self.selfDestructTime


    def getEsets(self):
        return self.sets['eset'] 

    def __repr__(self):
        return "TriggerData count: %s, timout: %s, args: %s, sets: %s" % (self.count, self.selfDestructTime, self.args, self.sets)


class Trigger(TriggerData):
    """
        Represents a trigger that the event stream wants to wait on.
        May include a timeout to continue regardless of completion.
    """
    def __init__(self, timeout=sys.maxint, target=None, **kwargs):
        """ Create a new trigger object from the aal """
        self.timeout = timeout
        self.target = target
        TriggerData.__init__(self, kwargs)

    def getEsets(self):
        return TriggerData.getEsets(self) 

    def __repr__(self):
        return "Trigger target:%s, timeout:%s, data: %s" % (
            self.target, self.timeout, TriggerData.__repr__(self))


class TriggerList(list):
    """
        The grouping of triggers that we find in an AAL entry
    """
    def __init__(self, triggerlist=None):
        for entry in triggerlist:
            self.append(Trigger(**entry))

    def getEsets(self):
        completeests = set() 
        for entry in self:
            tesets = Trigger.getEsets(entry) 
            if tesets:
                for e in tesets:
                    completeests.add(e)
        return completeests

    def __repr__(self):
        rstr = 'TriggerList: \n\t'
        for entry in self:
            rstr = rstr + Trigger.__repr__(entry)
            rstr = rstr + '\n\t'
        rstr = rstr + '\n' 
        return rstr 


class EventObject(object):
    """ Represents anything that sits in the event queue and can be sent.  """
    def getMessages(self):
        """ Get the MAGIMessage's that can be sent on the wire """
        return None


class BaseMethodCall(EventObject):
    """ Base class for any thing that sends an encoded MethodCall """

    def __init__(self, groups=None, nodes=None, docks=None,
                 method=None, args=None, trigger=None):
        """
            Create a new method call object.
            groups - list of groups to send to
            nodes - list of nodes to send to
            docks - list of destination docks
            method - method name
            args - dictionary of keyword arguments
            trigger - (optional) the string to send back as a trigger once the
                call is complete
        """
        self.groups = groups
        self.nodes = nodes
        self.docks = docks

        self.method = method
        self.args = args

        self.trigger = trigger

    def getMessages(self):
        call = {'version': 1.0, 'method': self.method, 'args': self.args}
        if self.trigger:
            call['trigger'] = self.trigger
        return [MAGIMessage(groups=self.groups, nodes=self.nodes,
                            docks=self.docks, contenttype=MAGIMessage.YAML,
                            data=yaml.dump(call))]

    def __repr__(self):
        return 'Event: %s(%s) \n\t trigger: %s\n' % (self.method, self.args, self.trigger)

class EventMethodCall(BaseMethodCall):
    """ MethodCall class for sending regular method calls from AAL events """

    def __init__(self, aalagent, aalevent):
        # trigger is optional
        trigger = None if 'trigger' not in aalevent else aalevent['trigger']
        BaseMethodCall.__init__(self, groups=aalagent['group'],
                                docks=aalagent['dock'],
                                method=aalevent['method'],
                                args=aalevent['args'], trigger=trigger)
    def getEventTrigger(self):
        return self.trigger


class LoadUnloadAgentCall(BaseMethodCall):
    """ MethodCall class for sending loadAgent or unloadAgent requests
    when starting agents """

    def __init__(self, load, name, **kwargs):
        """
            Create the load/unload agent call, expected kwargs 'code'
            and 'execargs'
            Optional kwargs are 'tardata' or 'path'
        """
        args = {
            "name": name,
            "code": kwargs['code'],
            "dock": kwargs['dock'],
            "execargs": kwargs['execargs']
        }

        # optional values, but only 1
        if "tardata" in kwargs:
            args["tardata"] = kwargs["tardata"]
        elif "path" in kwargs:
            args["path"] = kwargs["path"]

        if "idl" in kwargs:
            args["idl"] = kwargs["idl"]

        method = 'loadAgent' if load else 'unloadAgent'
        BaseMethodCall.__init__(self, groups=kwargs['group'], docks='daemon',
                                method=method, args=args)


class LoadAgentCall(LoadUnloadAgentCall):
    '''Call upon the daemon to load the given agent.'''
    def __init__(self, name, **kwargs):
        LoadUnloadAgentCall.__init__(self, True, name, **kwargs)


class UnloadAgentCall(LoadUnloadAgentCall):
    '''Call upon the daemon to unload the given agent.'''
    def __init__(self, name, **kwargs):
        LoadUnloadAgentCall.__init__(self, False, name, **kwargs)


# TODO: add intellegence to build and destroy groups as they are used or unused
class GroupCall(BaseMethodCall):
    """" Base class for joinGroup, leaveGroup calls. """
    def __init__(self, load, name, nodes):
        args = {
            "group": name,
            "nodes": nodes
        }
        method = 'joinGroup' if load else 'leaveGroup'
        BaseMethodCall.__init__(self, groups="__ALL__", docks='daemon',
                                method=method, args=args)


class BuildGroupCall(GroupCall):
    """" MethodCall class for sending joinGroup requests when
    joining down groups """
    def __init__(self, name, nodes):
        GroupCall.__init__(self, True, name, nodes)


class LeaveGroupCall(GroupCall):
    """" MethodCall class for sending leaveGroup requests when
    tearing down groups """
    def __init__(self, name, nodes):
        GroupCall.__init__(self, False, name, nodes)



class AAL(object):
    """
        A parsed AAL File which an internal list that contains the
        ordered set of events and triggers
    """

    def __init__(self, files=None, data=None, groupBuildTimeout=20000, dagdisplay=False):
        """
            Create a new AAL object using either files or a
            string object (data).
            The init function parses the yaml file and creates 
            the list of events and triggers that form each event stream
            ADditionally, it also creates the control graph that can be 
            visualized later.
        """
        
        if dagdisplay:
            print "dagdisplay True, creating graph" 
            self.cgraph = ControlGraph() 
            print self.cgraph 
            # the setup and tearm down events are presented as singleton events 

                       
        yaml_file = cStringIO.StringIO()
        read_data = False
        for f in files:
            # we concatenate the given files. 
            # This allows us to parse all the files as a single YAML
            # string. PyYAML does not support multidocument YAML 
            # documents, otherwise we could separate these files explicitly
            # with the yaml document separator, '---'. 
            with open(f, 'r') as fd:
                yaml_file.write(fd.read())
                read_data = True

        if not read_data:  # There is a more elegant way to do this.
            log.critical('Yaml Parse Error: reading event AAL files.')
            sys.exit(1)

        self.aal = yaml.load(yaml_file.getvalue())
        self.setupStream = list()
        self.teardownStream = list()
        self.streams = dict()
        self.eventtriggers = defaultdict(set) 

        # The AAL extra-YAML references
        self._resolveReferences(self.aal)

        # sanity check.
        for k in ['streamstarts', 'agents', 'groups', 'eventstreams']:
            if not k in self.aal.keys():
                log.critical('missing required key in AAL: %s', k)
                #sys.exit(1)

        # Stand up the experiemnt, load agents, build groups.
        for name, nodes in self.aal['groups'].iteritems():
            self.setupStream.append(BuildGroupCall(name, nodes))
        for name, nodes in self.aal['groups'].iteritems():
            self.setupStream.append(
                TriggerList([
                    {'event': 'GroupBuildDone', 'group': name, 'nodes': nodes},
                    {'event': 'GroupBuildDone', 'retVal': False,
                     'target': 'exit'},
                    {'timeout': int(groupBuildTimeout), 'target': 'exit'}]))

        # create an internal agent dock using unique name of agent. 
        # if specified in the AAL, do not do this. 
        for name, agent in self.aal['agents'].iteritems():
            if not 'dock' in agent:
                agent['dock'] = name + '_dock'
            if not 'code' in agent:
                agent['code'] = name + '_code' 

        for name, agent in self.aal['agents'].iteritems():
            self.setupStream.append(LoadAgentCall(name, **agent))
        for name, agent in self.aal['agents'].iteritems():
            timeout = (200000 if not 'loadTimeout' in agent
                       else agent['loadTimeout'])
            self.setupStream.append(
                TriggerList([
                    {'timeout': timeout, 'event': 'AgentLoadDone',
                     'agent': name,
                     'nodes': self.aal['groups'][agent['group']]},
                    {'event': 'AgentLoadDone', 'agent': name, 'retVal': False,
                     'target': 'exit'}]))

        # tear down the experiment, unload agents, leave groups.
        for name, agent in self.aal['agents'].iteritems():
            self.teardownStream.append(UnloadAgentCall(name, **agent))
        for name, agent in self.aal['agents'].iteritems():
            # unload timeout is less important so a default is probably OK.
            self.teardownStream.append(
                TriggerList([{'timeout': 200000, 'event': 'AgentUnloadDone',
                              'agent': name,
                              'nodes': self.aal['groups'][agent['group']]}]))

        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(LeaveGroupCall(name, nodes))
        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(
                TriggerList([{'timeout': 20000, 'event': 'GroupTeardownDone',
                              'group': name, 'nodes': nodes}]))

        #if dagdisplay: 
            #callgraph = pydot.Dot(graph_type='digraph',fontname="Verdana")
            #callgraph.add_node(pydot.Node('Setup',label='Setup'))
            #callgraph.add_node(pydot.Node('TearDown', label='TearDown'))
            #c = dict()
            #firstmessages = list()  

        for key, estream in self.aal['eventstreams'].iteritems():
            newstream = list()
            self.streams[key] = newstream
            if dagdisplay:
                #c[key]=pydot.Cluster(key,label=key)
                self.cgraph.createCluster(key) 

            for event in estream:
                # The eventstream consists of triggers and events. 
                # First we process the type trigger, then event. 
                # we log errors if it is not an event or trigger. 

                if event['type'] == 'trigger':
                    newstream.append(TriggerList(event['triggers']))

                    feset = TriggerList(event['triggers']).getEsets()
                    if feset:
                        for f in feset:
                            self.eventtriggers[key].add(f)

                    if dagdisplay:
                        self.cgraph.addTrigger(key,event['triggers'])

                elif event['type'] == 'event':
                    agent = self.aal['agents'][event['agent']]
                    newstream.append(EventMethodCall(agent, event))
                    if 'trigger' in event:
                        self.eventtriggers[key].add(event['trigger'])
                    if dagdisplay: 
                        self.cgraph.addEvent(key,event)
                        # Error: it is not an event or a trigger type     
                else:
                    log.warning("Skipping unknown stream entry type %s",
                                event['type'])

            if dagdisplay:
                self.cgraph.finishCluster(key)

        if dagdisplay:
        # The for loop ends, the current eventstream is processed 
#       # Create a call graph for the curent event stream
            self.cgraph.finishControlgraph()
            self.cgraph.writepng()
            #print self.cgraph
            


    def getSetupStream(self):
        return self.setupStream

    def getTeardownStream(self):
        return self.teardownStream

    def getStream(self, key):
        """ Get the event stream at a particular index """
        return self.streams[key]

    def hasStream(self, key):
        """ Return true if the event stream contains a particular stream """
        return key in self.streams

    def getStartKeys(self):
        """ Get the stream keys that should be started in parallel """
        return self.aal['streamstarts']

    def getTotalStartStreams(self):
        return len(self.aal['streamstarts']) 

    def getTotalStreams(self):
        return len(self.aal['eventstreams'])

    def getInterStreamEvents(self):
        ies = set()
        for k in self.eventtriggers:
            if len(ies) == 0:
                ies = self.eventtriggers[k]
            else:
                ies.union(self.eventtriggers[k])
        return ies 


    def __repr__(self):
        print "Setup Stream" 
        print self.setupStream
        print "Stream\n\n" 
        for s in self.getStartKeys():
            print s 
            print self.streams[s] 
        print "Teardown Stream" 
        print self.teardownStream


    def _resolveReferences(self, aal):
        '''
        The Agent AAL file syntax allows extra-YAML references. This
        method iterates over the parsed YAML structure and resolves the
        references 'by hand'. Call once on an already parsed (load()ed)
        YAML file.

        Arg: aal, the loaded aal file to resolve.
        '''
        # The only extra-YAML reference right now is 'agent' in the
        # event stream triggers. So find those are modify to the list
        # of nodes in the group.
        # aal['eventstreams']['triggers]['agent'] --> aal['groups']['nodes']

        # Sanity check
        if ('agents' not in aal or 'groups' not in aal or
                'eventstreams' not in aal):
            raise AALParseError('agents or groups or eventstreams not found in'
                                'AAL file. Unable to continue.')

        for stream in aal['eventstreams']:
            for event in aal['eventstreams'][stream]:
                if event['type'] == 'trigger':
                    for trigger in event['triggers']:
                        if 'agent' in trigger:
                            agent = trigger['agent']
                            if agent not in aal['agents']:
                                raise AALParseError('Agent "%s" referenced in'
                                                    'trigger %s does not '
                                                    'appear in AAL file.' %
                                                    (agent, trigger))

                            # Got the agent, find the group
                            if 'group' not in aal['agents'][agent]:
                                raise AALParseError('No "group" found in agent'
                                                    ' %s' % agent)

                            group = aal['agents'][agent]['group']
                            if group not in aal['groups']:
                                raise AALParseError('Unable to find group %s '
                                                    'in groups.' % group)

                            # Got the group, find the nodes.
                            nodes = aal['groups'][group]

                            # Got the nodes, do the substitution.
                            del trigger['agent']
                            trigger['nodes'] = nodes



class ControlGraph(object):
    """ Create the call graph from the procedure description 
    """
    def __init__(self):
        print "in Controlgraph" 
        self.controlgraph = dict() 
        self.keys = list()
        self.addSetupNode()
        self.keys.append('setup')
        self.addTeardownNode()
        self.keys.append('teardown')

    def addSetupNode(self):
        self.controlgraph['setup'] = {'id': 'setup0', 'label': 'Setup', 'type': 'node'}  

    def addTeardownNode(self):
        self.controlgraph['teardown'] = {'id': 'exit0', 'label': 'TearDown', 'type': 'node'}  


    def createCluster(self,key):
        self.keys.append(key)
        self.controlgraph[key] = ControlGraphCluster(key) 

    def writepng(self):
        import pydot 
        # TODO: walk through the controlgraph 
        # keys. if key is setup or teardown add node
        # else add cluster and send to graphcluster to populate 
        cpng = pydot.Dot(graph_type = 'digraph', fontname="Verdana")
        for k in self.keys:
            if k == 'setup' or k == 'teardown':
                cpng.add_node(pydot.Node(self.controlgraph[k]['id'],
                                         label = self.controlgraph[k]['label']))
            else:
                c = pydot.Cluster(k, label=k)

                allnodes = self.controlgraph[k].getAllNodes()
                for n in allnodes:
                    if n['type'] != 'syncnode':
                        c.add_node(pydot.Node(n['id'], label=n['label']))

                cpng.add_subgraph(c)

                alledges = self.controlgraph[k].getAllEdges()
                for e in alledges:
                    cpng.add_edge(pydot.Edge(e['from'], e['to'], label=e['label']))

        cpng.write_raw('test.dot')
        cpng.write_png('test.png')



    def finishControlgraph(self):
        # if addEvent edge to node exisits otherwise add external node to sink edge 
        # if addTrigger edge from node exisits otherwise add external node generating
        pass 

    def finishCluster(self,key):
        self.addIntraClusterEdges(key)
        # if triggerlist, need to add proper edges from parent event
        

    def addIntraClusterEdges(self,key):
        self.controlgraph[key].addStraightEdges()

    def addEvent(self,key,event):
        self.controlgraph[key].addEvent(event)

    def addTrigger(self,key,event):
        self.controlgraph[key].addTrigger(event)

    def __repr__(self):
        rstr = "Control Graph\n" 
        for k, v in self.controlgraph.items():
            rstr = rstr + str(k) + "\n" + v.__repr__()+ "\n"
        return rstr 

class ControlGraphCluster(dict):
    """ Creates a cluster of connected control graph nodes  
    """

    def __init__(self,key):
        self.key = key
        self.eventcount = 0 
        self.targettriggers = list() 
        self.nodes = list()
        self.edges = list()


    def getWaitTriggers(self):
        return self.targetriggers

    def getNodeIndex(self):
        return len(self.nodes)

    def getAllNodes(self):
        return self.nodes 

    def getEdgeIndex(self):
        return len(self.edges)

    def getAllEdges(self):
        return self.edges

    def addEvent(self,event):
        print "adding node", event 
        tempnode = dict()
        tempnode['type'] = 'node'
        tempnode['id']= str(self.key) + "Event" + str(self.eventcount) 
        # Create the label 
        label = "Send" 
        # TODO: write send method to agent format using getkey 
        for k,v in event.items():
            if k == 'trigger':
                    tempnode['edgeto']= v 
                    self.addOneEdge(tempnode['id'], ('Trigger' + str(v)),label=str(v)) 
                    continue
            if k == 'args' or k == 'execargs' or k == 'type':
                continue 
            label = label + "\n" + str(k) + ":" + str(v)

        tempnode['label'] = label 
        self.nodes.append(tempnode)
        self.advanceEventCount()
                
    def addStraightEdges(self):
        prevnodes = list() 
        prevnodes.append('setup0')

        currentnodes = list() 
        # index into nodes 
        j = 0 

        for i in range(0, self.eventcount):
            if 'Event' in self.nodes[j]['id']:
                currentnodes.append(self.nodes[j]['id'])
                j += 1
            else:
                if 'StartTList' in self.nodes[j]['id']: 
                    # Ignore the syncnode 
                    j += 1
                    while not 'EndTList' in self.nodes[j]['id']:
                        currentnodes.append(self.nodes[j]['id'])
                        j += 1
                    # Ignore the syncnode 
                    j += 1

            if j > self.getNodeIndex():
                log.error("node index exceeds event index") 

            for pnode in prevnodes:
                for cnode in currentnodes:
                    tempedge = dict()
                    tempedge['id'] = str(self.key) + 'Edge' + str(self.getEdgeIndex())
                    tempedge['type'] = 'edge'
                    tempedge['label'] = " " 
                    # Test for first node in the stream 
                    # The first node gets an incoming edge from setup 
                    tempedge['to'] =  cnode
                    tempedge['from'] = pnode
                    self.edges.append(tempedge)
            prevnodes = currentnodes
            currentnodes = list() 


    def addOneEdge(self,fromNode,toNode,label=" "):
        tempedge = dict()
        tempedge['id'] = str(self.key) + 'Edge' + str(self.getEdgeIndex())
        tempedge['type'] = 'edge'
        tempedge['label'] = label 
        tempedge['from'] = fromNode
        tempedge['to'] = toNode 
        self.edges.append(tempedge)

    def addTrigger(self,trigger):
        print "adding trigger", trigger 
        # A Trigger is a list of TriggerData
        # We parse each trigger and see which one arrives and decides the control 
        # path 
        
        # Add triggerlist ends in the node list to indicate start and end of 
        # the trigger list #
        # This significantly simpliies adding and managing the links during 
        # fanin and fanout  
        tempnode = dict()
        tempnode['type'] = 'syncnode'
        tempnode['id'] = 'StartTList'
        self.nodes.append(tempnode)

        for i, l in enumerate(trigger):
            print l,i
            tempnode = dict()
            tempnode['type'] = 'node' 
            tempnode['id'] = str(self.key) + "Trigger"+ str(self.eventcount) + str(i)
            label = "Wait Until"
            if l.get('event'):
                tempnode['edgefrom'] = l.get('event')
                label = label + "\n" + 'event: ' + str(l.get('event')) 
                # if this is a trigger waiting from an event then we set the 
                # id with the event name 
                tempnode['id'] = 'Trigger' + str(l.get('event'))
            if 'target' in l:
                # These targers are eventstreams 
                # Hence we can directly add a edge here 
                tempnode['edgeto'] = l['target']
                if l.get('event'):
                    labelstr = l.get('event') 
                else: 
                    labelstr = "jump"
                self.addOneEdge(tempnode['id'], (str(l['target']) + '0'), label=labelstr)
            if l.get('timeout') == sys.maxint:
                label  = label + "\n" + 'timeout: Never'
            elif l.get('timeout') is not None:
                label  = label + "\n" + 'timeout: ' + str(l.get('timeout')/1000) + "s"
            tempnode['label'] = label 
            self.nodes.append(tempnode)

        tempnode = dict()
        tempnode['type'] = 'syncnode'
        tempnode['id'] = 'EndTList'
        self.nodes.append(tempnode)
        self.advanceEventCount()

    def advanceEventCount(self):
        self.eventcount = self.eventcount + 1

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


# locally parse AAL file-$                                                     
if __name__ == "__main__":
    optparser = optparse.OptionParser()
    optparser.add_option("-f", "--file", dest="file", help="AAL Events file", default=[], action="append")
    (options, args) = optparser.parse_args()

    x = AAL(files=options.file, dagdisplay=True)
    print "croo event triggers", x.eventtriggers
    #print x.__repr__()


