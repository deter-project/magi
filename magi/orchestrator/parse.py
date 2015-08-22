#!/usr/bin/env python

import cStringIO
from collections import defaultdict
import logging
import optparse
import sys
import time

from magi.messaging.api import MAGIMessage
from magi.util import helpers
import yaml

from controlflow import ControlGraph  


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
    { type: trigger, triggers: [ esets: [ { event:X, otherkey:Y } { event:Y, otherkey:A } ], target: null, timeout: 100} 
        ...] }

"""

class Trigger(object):
    """
        Represents a trigger that the event stream wants to wait on.
        May include a timeout to continue regardless of completion.
    """
    
    TIMEOUT = 1
    EVENT = 2
    CONJUNCTION = 3
    DISJUNCTION = 4
        
    def __init__(self, triggerData):
        """ Create a new trigger object from the aal """
        self.target = triggerData.pop('target', None)
        self.active = False
        self.timeActivated = None
        
    def activate(self, activationTime=None):
        if not activationTime:
            activationTime = time.time()
        self.active = True
        self.timeActivated = activationTime
    
    def deActivate(self, activationTime=None):
        self.active = False
        
    def isActive(self):
        return self.active
    
    def getTimeout(self):
        return 0

    def __repr__(self):
        return "{ \n\tTrigger type: %s \n\tTrigger data: %s \n}" % (
                self.__class__.__name__, self.__dict__)

    def __eq__(self, other): 
        return self.__dict__ == other.__dict__
    
    def __hash__(self):
        return hash(self.__dict__)

    def toString(self):
        return self.__repr__()
    
class TimeoutTrigger(Trigger):
    
    def __init__(self, triggerData):
        Trigger.__init__(self, triggerData)
        self.timeout = triggerData.get('timeout', 0) / 1000
        
    def isComplete(self, triggerCache=None):
        if self.isActive():
            return time.time() >= (self.timeActivated + self.timeout)
        return False
    
    def getTimeout(self):
        if self.isActive():
            return (self.timeActivated + self.timeout)
        return sys.maxint
    
    def __eq__(self, other): 
        return (self.timeout == other.timeout) and \
                (self.target == other.target)
    
    def __hash__(self):
        return hash((self.timeout, self.target))
    
    def toString(self):
        return "%d seconds" %(self.timeout)
    
class EventTrigger(Trigger):
    
    def __init__(self, triggerData):
        functionName = self.__init__.__name__
        helpers.entrylog(log, functionName, locals())
    
        Trigger.__init__(self, triggerData)
        self.event = triggerData.pop('event')
        self.nodes = helpers.toSet(triggerData.pop('nodes', None))
        self.count = triggerData.pop('count', max(len(self.nodes), 1))
        self.args = triggerData
        if not self.args:
            self.args = {'retVal' : True}
        
    def isComplete(self, triggerCache):
        if not triggerCache:
            return False
        
        if self.event in triggerCache:
            cachedTriggers = triggerCache[self.event]
            matchingTriggers = []
            
            for cachedTrigger in cachedTriggers:
                match = True
                for key, value in self.args.iteritems():
                    if cachedTrigger.args.get(key) != value:
                        match = False
                        break
                if match:
                    matchingTriggers.append(cachedTrigger)
            
            interestedNodeSet = set()
            
            for matchingTrigger in matchingTriggers:
                if self.nodes:
                    interestedNodeSet |= self.nodes.intersection(matchingTrigger.nodes)
                else:
                    interestedNodeSet |= matchingTrigger.nodes
                
            return len(interestedNodeSet) >= self.count
        
        return False
    
    def isEqual(self, trigger):
        return self.event == trigger.event and self.args == trigger.args
    
    def merge(self, trigger):
        self.nodes.update(trigger.nodes)
        self.count = max(len(self.nodes), 1)
    
    def __eq__(self, other): 
        return (self.event == other.event) and \
                (self.args == other.args) and \
                (self.target == other.target)
                 
    def __hash__(self):
        return hash((self.event, yaml.dump(self.args), self.target))
    
    def toString(self):
        return "%s" %(self.event)
    
class ConjunctionTrigger(Trigger):
    
    def __init__(self, triggerData):
        Trigger.__init__(self, triggerData)
        self.triggers = TriggerList(triggerData['triggers'])
        
    def activate(self, activationTime=None):
        if not activationTime:
            activationTime = time.time()
        Trigger.activate(self, activationTime)
        for trigger in self.triggers:
            trigger.activate(activationTime)
            
    def isComplete(self, triggerCache=None):
        for trigger in self.triggers:
            if not trigger.isComplete(triggerCache):
                return False
        return True
    
    def getTimeout(self):
        return max([trigger.getTimeout() for trigger in self.triggers])

    def toString(self):
        trgrRepr = " & ".join([trigger.toString() for trigger in self.triggers])
        return "(%s)" %(trgrRepr)
    
class DisjunctionTrigger(Trigger):
    
    def __init__(self, triggerData):
        Trigger.__init__(self, triggerData)
        self.triggers = TriggerList(triggerData['triggers'])

    def activate(self, activationTime=None):
        if not activationTime:
            activationTime = time.time()
        Trigger.activate(self, activationTime)
        for trigger in self.triggers:
            trigger.activate(activationTime)
                            
    def isComplete(self, triggerCache=None):
        for trigger in self.triggers:
            if trigger.isComplete(triggerCache):
                return True
        return False
    
    def getTimeout(self):
        return min([trigger.getTimeout() for trigger in self.triggers])

    def toString(self):
        trgrRepr = " or ".join([trigger.toString() for trigger in self.triggers])
        return "(%s)" %(trgrRepr)
                    
def getTriggerType(triggerData):
    try:
        if 'event' in triggerData:
            return Trigger.EVENT
        elif 'type' in triggerData:
            if triggerData['type'] == 'AND':
                return Trigger.CONJUNCTION
            elif triggerData['type'] == 'OR':
                return Trigger.DISJUNCTION
        return Trigger.TIMEOUT
    except:
        pass
    
def createTrigger(triggerData):
    log.debug('Creating new trigger')
    triggerClasses = { Trigger.TIMEOUT : TimeoutTrigger, 
                       Trigger.EVENT : EventTrigger, 
                       Trigger.CONJUNCTION : ConjunctionTrigger, 
                       Trigger.DISJUNCTION : DisjunctionTrigger }
    
    triggerType = getTriggerType(triggerData)
    log.debug('Trigger Type: %s' %(triggerType))
    
    return triggerClasses[triggerType](dict(triggerData))
    
def getEventTriggers(triggers):
    triggers = helpers.toSet(triggers)
    eventTriggers = set()
    for trigger in triggers:
        if isinstance(trigger, TimeoutTrigger):
            continue
        elif isinstance(trigger, EventTrigger):
            eventTriggers.add(trigger)
        else:
            eventTriggers.update(getEventTriggers(trigger.triggers))
    return eventTriggers

class TriggerList(list):
    """
        The grouping of triggers that we find in an AAL entry
    """
    def __init__(self, triggerlist=[]):
        for entry in triggerlist:
            self.append(createTrigger(dict(entry))) #cloning entry to not change the original
        
    def activate(self, activationTime=None):
        if not activationTime:
            activationTime = time.time()
        for trigger in self:
            trigger.activate(activationTime)
            
    def getTimeout(self):
        return min([trigger.getTimeout() for trigger in self])
    
    def __repr__(self):
        return 'TriggerList: %s \n' %(list(self))

class EventObject(object):
    """ Represents anything that sits in the event queue and can be sent.  """
    def getMessages(self):
        """ Get the MAGIMessage's that can be sent on the wire """
        return None

    def __eq__(self, other):
        return self.__dict__ == other.__dict__
    
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
        
        self.agent = 'daemon'

    def getMessages(self):
        call = {'version': 1.0, 'method': self.method, 'args': self.args}
        if self.trigger:
            call['trigger'] = self.trigger
        return [MAGIMessage(groups=self.groups, nodes=self.nodes,
                            docks=self.docks, contenttype=MAGIMessage.YAML,
                            data=yaml.dump(call))]

    def __repr__(self):
        return 'Event: %s(%s) \n\t trigger: %s\n' %(self.method, 
                                                    self.args, 
                                                    self.trigger)

    
class EventMethodCall(BaseMethodCall):
    """ 
        MethodCall class for sending regular method calls from AAL events 
    """
    def __init__(self, aalagent, aalevent):
        BaseMethodCall.__init__(self, groups=aalagent['group'],
                                docks=aalagent['dock'],
                                method=aalevent['method'],
                                args=aalevent['args'], 
                                # trigger is optional
                                trigger=aalevent.get('trigger'))
        self.agent = aalevent['agent']

def createEvent(eventData, aal):
    if eventData['type'] == 'event':
        agent = aal['agents'][eventData['agent']]
        return EventMethodCall(agent, eventData)
    elif eventData['type'] == 'trigger':
        return TriggerList(eventData['triggers'])
    raise Exception("Invalid event data")

class LoadUnloadAgentCall(BaseMethodCall):
    """ 
        MethodCall class for sending loadAgent or unloadAgent requests
        when starting agents 
    """
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


# TODO: add intelligence to build and destroy groups as they are used or unused
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

class GroupPingCall(BaseMethodCall):
    """" Base class for ping, to check if groups have been built. """
    def __init__(self, group):
        args = {
            "group": group,
        }
        BaseMethodCall.__init__(self, groups=group, docks='daemon',
                                method='groupPing', args=args)
        
class Stream(list):
    
    def __init__(self, name, *args, **kwargs):
        self.name = name
        list.__init__(self, *args, **kwargs)
        
    def __repr__(self):
        return "%s:\n\n%s" %(self.name, list.__repr__(self))
        
        
class AAL(object):
    """
        A parsed AAL File which an internal list that contains the
        ordered set of events and triggers
    """

    def __init__(self, files=None, data=None, groupBuildTimeout=20000, dagdisplay=False, triggerCheck=False):
        """
            Create a new AAL object using either files or a
            string object (data).
            The init function parses the yaml file and creates 
            the list of events and triggers that form each event stream
            ADditionally, it also creates the control graph that can be 
            visualized later.
        """

        # TODO: currently the startup stream is always setup for an AAL 
        # Later the experiment may or may not have a startup phase 
        self.startup = True 
        self.agentLoadTimeout = 200000
        
        try:
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
    
            self.rawAAL = yaml.load(yaml_file.getvalue())
            
            #Pointer to streams
            self.setupStreams = []
            self.teardownStreams = []
            self.userEventStreams = []
            
            #Stream name to object map
            self.streamMap = dict()
            
            #Incoming event triggers keyed by stream name
            self.ieventtriggers = defaultdict(set) 
            #Outgoing event triggers keyed by stream name
            self.oeventtriggers = defaultdict(set) 
    
            # Sanity Check: does the AAL have the following directives. 
            # if not, log that they are missing but continue 
            for k in ['streamstarts', 'agents', 'groups', 'eventstreams']:
                if not k in self.rawAAL.keys():
                    log.critical('missing required key in AAL: %s', k)
    
            # Add default group to address ALL nodes
            allNodes = set()
            for nodes in self.rawAAL['groups'].values():
                allNodes |= helpers.toSet(nodes)
            self.rawAAL['groups']['__ALL__'] = list(allNodes)
            
            # Add MAGI Daemon on all nodes as a default agent
            self.rawAAL['agents'].setdefault('__DAEMON__', {'group' : '__ALL__',
                                                         'dock' : 'daemon'})
            
            # The AAL extra-YAML references
            self._resolveReferences()
            
            ##### STARTUP STREAM #####
            
            # Define startup stream 
            # By default we add a startup stream 
            if self.startup: 
            # Stand up the experiment, load agents, build groups.
            
                groupBuildStream = Stream('groupBuildStream')
                self.setupStreams.append(groupBuildStream)
                self.streamMap['groupBuildStream'] = groupBuildStream
                
                for name, nodes in self.rawAAL['groups'].iteritems():
                    if name == '__ALL__': continue # all nodes by default receive messages sent to the '__ALL__' group
                    groupBuildStream.append(BuildGroupCall(name, nodes))
    
                # Add triggers for the BuildGroup calls 
                for name, nodes in self.rawAAL['groups'].iteritems():
                    if name == '__ALL__': continue # no GroupBuild message sent for '__ALL__' group
                    groupBuildStream.append(
                        TriggerList([
                            {'event': 'GroupBuildDone', 'group': name, 
                             'nodes': nodes},
                            {'timeout': int(groupBuildTimeout), 
                             'target': 'exit'}]))
                
                for name, nodes in self.rawAAL['groups'].iteritems():
                    if name == '__ALL__': continue # all nodes by default receive messages sent to the '__ALL__' group
                    groupBuildStream.append(GroupPingCall(name))
                    
                # Add triggers for the BuildGroup calls 
                for name, nodes in self.rawAAL['groups'].iteritems():
                    if name == '__ALL__': continue # no GroupBuild message sent for '__ALL__' group
                    groupBuildStream.append(
                        TriggerList([
                            {'event': 'GroupPong', 'group': name, 
                             'nodes': nodes},
                            {'timeout': int(groupBuildTimeout), 
                             'target': 'groupBuildStream'}]))
                    
                loadAgentStream = Stream('loadAgentStream')
                self.setupStreams.append(loadAgentStream)
                self.streamMap['loadAgentStream'] = loadAgentStream
                
                for name, agent in self.rawAAL['agents'].iteritems():
                    # for agents that need to be installed
                    if 'path' in agent:
                        # create an internal agent dock using unique name of agent. 
                        # if specified in the AAL, do not do this. 
                        if not 'dock' in agent:
                            agent['dock'] = name + '_dock'
                        if not 'code' in agent:
                            agent['code'] = name + '_code' 
        
                        # Add event call to load agent
                        loadAgentStream.append(LoadAgentCall(name, **agent))
                        
                    else:
                        if not 'dock' in agent:
                            agent['dock'] = 'daemon'
                        
                # Now, add the load agent triggers
                for name, agent in self.rawAAL['agents'].iteritems():
                    # for agents that need to be installed
                    if 'path' in agent:
                        
                        # Add triggers to ensure the agents are loaded correctly 
                        # However, add them only after all the load agent events
                        timeout = agent.get('loadTimeout', self.agentLoadTimeout)
                        loadAgentStream.append(
                            TriggerList([
                                {'event': 'AgentLoadDone', 'agent': name, 
                                 'nodes': self.rawAAL['groups'][agent['group']]},
                                {'timeout': int(timeout), 'target': 'exit'} 
                                 ]))
    
            ##### TEARDOWN STREAM #####
            
            # We always define a teardown stream as jumping to target exit 
            # activates this stream 
            # tear down the experiment, unload agents, leave groups.
            
            unloadAgentStream = Stream('unloadAgentStream')
            self.teardownStreams.append(unloadAgentStream)
            self.streamMap['unloadAgentStream'] = unloadAgentStream
                
            # Add unload agent events
            for name, agent in self.rawAAL['agents'].iteritems():
                if 'path' in agent:
                    unloadAgentStream.append(UnloadAgentCall(name, **agent))
    
            # Add triggers to ensure the agents are unloaded correctly 
            # However, add them only after all the unload agent events
            # Use the same timeouts as setup stream
            for name, agent in self.rawAAL['agents'].iteritems():
                if 'path' in agent:
                    timeout = agent.get('loadTimeout', self.agentLoadTimeout)
                    unloadAgentStream.append(
                        TriggerList([
                                {'event': 'AgentUnloadDone', 'agent': name,
                                'nodes': self.rawAAL['groups'][agent['group']]},
                                {'timeout': int(timeout), 'target': 'exit'}
                                ]))
            
            groupLeaveStream = Stream('groupLeaveStream')
            self.teardownStreams.append(groupLeaveStream)
            self.streamMap['groupLeaveStream'] = groupLeaveStream
                
            # Add leave group events
            for name, nodes in self.rawAAL['groups'].iteritems():
                if name == '__ALL__': continue # no GroupBuild message sent for '__ALL__' group    
                groupLeaveStream.append(LeaveGroupCall(name, nodes))
                
            # Add triggers to ensure groups are left correctly
            for name, nodes in self.rawAAL['groups'].iteritems():
                if name == '__ALL__': continue # no LeaveGroup message sent for '__ALL__' group
                groupLeaveStream.append(
                    TriggerList([
                        {'event': 'GroupTeardownDone', 'group': name, 
                         'nodes': nodes},
                        {'timeout': int(groupBuildTimeout), 'target': 'exit'}
                        ]))
    
    
            ##### EVENT STREAMS #####
    
            for streamName, estream in self.rawAAL['eventstreams'].iteritems():
                newstream = Stream(streamName)
                self.userEventStreams.append(newstream)
                self.streamMap[streamName] = newstream
                for event in estream:
                    # The eventstream consists of triggers and events. 
                    # First we process the type trigger, then event. 
                    # we log errors if it is not an event or trigger.
                    if event['type'] == 'event':
                        agent = self.rawAAL['agents'][event['agent']]
                        newstream.append(EventMethodCall(agent, event))
                        if 'trigger' in event:
                            self.oeventtriggers[streamName].add(event['trigger'])
                            
                    elif event['type'] == 'trigger':
                        triggerList = TriggerList(event['triggers'])
                        newstream.append(triggerList)
                        self.ieventtriggers[streamName].update(set([trigger.event 
                                                             for trigger in 
                                                             getEventTriggers(triggerList)]))
                            
                    else:
                        log.warning("Skipping unknown stream entry type %s",
                                    event['type'])
            
            
            outGoingEventTriggers = set()
            for triggerSet in self.oeventtriggers.values():
                outGoingEventTriggers |= triggerSet
                
            inComingEventTriggers = set()
            for triggerSet in self.ieventtriggers.values():
                inComingEventTriggers |= triggerSet
                
            if inComingEventTriggers.issubset(outGoingEventTriggers):
                log.info('Incoming event triggers is a subset of outgoing event triggers')
            elif triggerCheck:
                log.error('Incoming event triggers are not a subset of outgoing event triggers')
                raise AALParseError('Incoming event triggers are not a subset of outgoing event triggers')
            
            self.cgraph = ControlGraph() 
            for eventStream in self.userEventStreams:
                streamName = eventStream.name
                cluster = self.cgraph.createCluster(streamName)
                cluster.nodes.append({'type' : 'node',
                                      'id' : streamName,
                                      'label' : streamName,
                                      'edgeto' : set()})
                for event in eventStream:
                    if isinstance(event, EventObject):
                        self.cgraph.addEvent(streamName, event)
                    elif isinstance(event, TriggerList):
                        self.cgraph.addTrigger(streamName, event)
                self.cgraph.finishCluster(streamName)
            # the setup and tear-down event streams are presented as singleton events
            self.cgraph.finishControlgraph()
    
            if dagdisplay:
                print "dagdisplay True, creating graph" 
                self.cgraph.writePng()    
                #print self.cgraph
                
        except Exception, e:
            import traceback
            exc_type, exc_value, exc_tb = sys.exc_info()
            log.error(''.join(traceback.format_exception(exc_type, exc_value, exc_tb)))
            raise AALParseError("Exception while parsing AAL: %s" %str(e))
            
    def getSetupStreams(self):
        return self.setupStreams

    def getTeardownStreams(self):
        return self.teardownStreams

    def getStream(self, key):
        """ Get the event stream at a particular index """
        return self.streamMap[key]

    def hasStream(self, key):
        """ Return true if the event stream contains a particular stream """
        return key in self.streamMap

    def getStartKeys(self):
        """ Get the stream keys that should be started in parallel """
        return self.rawAAL['streamstarts']

    def getTotalStartStreams(self):
        return len(self.rawAAL['streamstarts']) 

    def getTotalStreams(self):
        return len(self.rawAAL['eventstreams'])

    def _resolveReferences(self):
        '''
        The Agent AAL file syntax allows extra-YAML references. This
        method iterates over the parsed YAML structure and resolves the
        references 'by hand'. Call once on an already parsed (load()ed)
        YAML file.

        Arg: aal, the loaded aal file to resolve.
        '''
        # The only extra-YAML reference right now is 'agent' in the
        # event stream triggers. So find those and modify to the list
        # of nodes in the group.
        # aal['eventstreams']['triggers]['agent'] --> aal['groups']['nodes']

        # Sanity check
        if ('agents' not in self.rawAAL or 'groups' not in self.rawAAL or
                'eventstreams' not in self.rawAAL):
            raise AALParseError('agents or groups or eventstreams not found in'
                                'AAL file. Unable to continue.')
            
        # Map outgoing triggers to respective agents
        triggerToAgentMap = {}
        for stream in self.rawAAL['eventstreams']:
            for event in self.rawAAL['eventstreams'][stream]:
                if event['type'] == 'event':
                    if 'trigger' in event:
                        trigger = event['trigger']
                        agent = event['agent']
                        # Store trigger to agent mapping
                        triggerToAgentMap.setdefault(trigger, agent)
                    
        # Map agents to corresponding nodes
        agentToNodesMap = {}
        for agent in self.rawAAL['agents']:
            # For the agent, find the group
            if 'group' not in self.rawAAL['agents'][agent]:
                raise AALParseError('No "group" found in agent'
                                    ' %s' % agent)

            group = self.rawAAL['agents'][agent]['group']
            if group not in self.rawAAL['groups']:
                raise AALParseError('Unable to find group %s '
                                    'in groups.' % group)

            # Got the group, find the nodes.
            nodes = set(self.rawAAL['groups'][group])
            
            # Store trigger to nodes mapping
            agentToNodesMap[agent] = nodes

                                                            
        def updateTrigger(triggerData):
            triggeType = getTriggerType(triggerData)
            if triggeType in [Trigger.TIMEOUT]:
                pass
            elif triggeType in [Trigger.EVENT]:
                triggerEvent = triggerData['event']
                if triggerEvent not in triggerToAgentMap:
                    log.warning('No outgoing event for trigger '
                                        '"%s"' %(triggerEvent))
                    return
                #triggerData.setdefault('agent', triggerToAgentMap[triggerEvent])
                #triggerAgent = triggerData['agent']
                triggerAgent = triggerData.get('agent', triggerToAgentMap[triggerEvent])
                if triggerAgent not in agentToNodesMap:
                    raise AALParseError('Outgoing trigger "%s" mapped to '
                                        'non-existing agent "%s"' %(triggerEvent, 
                                                                    triggerAgent))
                triggerData.setdefault('nodes', agentToNodesMap[triggerAgent])
            else:
                for trigger in triggerData['triggers']:
                    updateTrigger(trigger)

        # Update incoming triggers with agents and nodes
        for stream in self.rawAAL['eventstreams']:
            for event in self.rawAAL['eventstreams'][stream]:
                if event['type'] == 'trigger':
                    for trigger in event['triggers']:
                        updateTrigger(trigger)                

    def __repr__(self):
        rstr = "Setup Streams\n\n" 
        for stream in self.setupStreams:
            rstr += str(stream)
            rstr += "\n\n"
        rstr += "\n\nEvent Streams\n\n" 
        for stream in self.userEventStreams:
            rstr += str(stream)
            rstr += "\n\n"
        rstr += "Teardown Streams\n\n" 
        for stream in self.teardownStreams:
            rstr += str(stream)
            rstr += "\n\n"
        return rstr

      
class AALParseError(Exception):
    '''Small wrapper around exception for AAL parse errors.'''
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return repr(self.error)
    
if __name__ == "__main__":
    optparser = optparse.OptionParser()
    optparser.add_option("-f", "--file", dest="file", help="AAL Events file", default=[], action="append")
    (options, args) = optparser.parse_args()
    print options.file

    x = AAL(files=options.file, dagdisplay=True)
    print x.__repr__()

