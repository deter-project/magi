import logging
import yaml
import sys
import cStringIO
import optparse
from controlflow import ControlGraph  
from collections import defaultdict
from magi.messaging.api import MAGIMessage
from magi.util import helpers
import time

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

class Trigger():
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
        
    def activate(self, activationTime=None):
        self.active = True
    
    def isActive(self):
        return self.active
    
    def getTimeout(self):
        return 0

    def __repr__(self):
        return "{ \n\tTrigger type: %s \n\tTrigger data: %s \n}" % (
                self.__class__.__name__, self.__dict__)

class TimeoutTrigger(Trigger):
    
    def __init__(self, triggerData):
        Trigger.__init__(self, triggerData)
        self.timeout = triggerData['timeout'] / 1000
        self.timeActivated = None
        
    def activate(self, activationTime=None):
        if not activationTime:
            activationTime = time.time()
        Trigger.activate(self, activationTime)
        self.timeActivated = activationTime
        
    def isComplete(self, triggerCache=None):
        if self.isActive():
            return time.time() >= (self.timeActivated + self.timeout)
        return False
    
    def getTimeout(self):
        if self.isActive():
            return (self.timeActivated + self.timeout)
        return sys.maxint
    
class EventTrigger(Trigger):
    
    def __init__(self, triggerData):
        Trigger.__init__(self, triggerData)
        self.event = triggerData.pop('event')
        self.nodes = helpers.toSet(triggerData.pop('nodes', None))
        self.count = triggerData.pop('count', max(len(self.nodes), 1))
        triggerData.setdefault('result', True)    
        self.args = triggerData
        
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
                
#            if self.event == 'intfSensed':
#                print 'isComplete'
#                print self
#                print cachedTriggers
#                print matchingTriggers
#                print interestedNodeSet
                
            return len(interestedNodeSet) >= self.count
        
        return False
        
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
                
def getTriggerType(triggerData):
    try:
        if 'timeout' in triggerData:
            return Trigger.TIMEOUT
        elif 'event' in triggerData:
            return Trigger.EVENT
        elif 'type' in triggerData:
            if triggerData['type'] == 'AND':
                return Trigger.CONJUNCTION
            elif triggerData['type'] == 'OR':
                return Trigger.DISJUNCTION
    except:
        pass
    
    raise AttributeError("Invalid trigger data: %s" %(triggerData))

def createTrigger(triggerData):
    triggerClasses = { Trigger.TIMEOUT : TimeoutTrigger, 
                       Trigger.EVENT : EventTrigger, 
                       Trigger.CONJUNCTION : ConjunctionTrigger, 
                       Trigger.DISJUNCTION : DisjunctionTrigger }
    
    return triggerClasses[getTriggerType(triggerData)](triggerData)
    
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
            self.append(createTrigger(dict(entry)))
        
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


class Stream(list):
    
    def __init__(self, *args, **kwargs):
        list.__init__(self, *args, **kwargs)
        
    def gettriggerToAgentMaps(self):
        result = {}
        for i in  range(0, len(self.wrapped)):
            item = self[i]
            if isinstance(item, EventObject):
                if item.trigger:
                    result[item.trigger] = item
            

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

        # TODO: currently the startup stream is always setup for an AAL 
        # Later the experiment may or may not have a startup phase 
        self.startup = True 
        self.agentLoadTimeout = 200000
        
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
        self.setupStream = Stream()
        self.teardownStream = Stream()
        self.streams = dict()
        self.ieventtriggers = defaultdict(set) 
        self.oeventtriggers = defaultdict(set) 

        # Sanity Check: does the AAL have the following directives. 
        # if not, log that they are missing but continue 
        for k in ['streamstarts', 'agents', 'groups', 'eventstreams']:
            if not k in self.aal.keys():
                log.critical('missing required key in AAL: %s', k)

        # The AAL extra-YAML references
        self._resolveReferences(self.aal)
        
        ##### STARTUP STREAM #####
        
        # Define startup stream 
        # By default we add a startup stream 
        if self.startup: 
        # Stand up the experiemnt, load agents, build groups.
            for name, nodes in self.aal['groups'].iteritems():
                self.setupStream.append(BuildGroupCall(name, nodes))

            # Add triggers for the BuildGroup calls 
            for name, nodes in self.aal['groups'].iteritems():
                self.setupStream.append(
                    TriggerList([
                        {'event': 'GroupBuildDone', 'group': name, 
                         'nodes': nodes},
                        {'event': 'GroupBuildDone', 'result': False, 
                         'target': 'exit'},
                        {'timeout': int(groupBuildTimeout), 
                         'target': 'exit'}]))

            # create an internal agent dock using unique name of agent. 
            # if specified in the AAL, do not do this. 
            for name, agent in self.aal['agents'].iteritems():
                if not 'dock' in agent:
                    agent['dock'] = name + '_dock'
                if not 'code' in agent:
                    agent['code'] = name + '_code' 

            # Add event call for load Agents 
            for name, agent in self.aal['agents'].iteritems():
                self.setupStream.append(LoadAgentCall(name, **agent))

            # Add triggers to ensure  the agents are loaded correctly 
            for name, agent in self.aal['agents'].iteritems():
                timeout = agent.get('loadTimeout', self.agentLoadTimeout)
                self.setupStream.append(
                    TriggerList([
                        {'event': 'AgentLoadDone', 'agent': name, 
                         'nodes': self.aal['groups'][agent['group']]},
                        {'event': 'AgentLoadDone', 'agent': name, 
                         'result': False, 'target': 'exit'},
                        {'timeout': int(timeout), 'target': 'exit'} 
                         ]))


        ##### TEARDOWN STREAM #####
        
        # We always define a teardown stream as jumping to target exit 
        # activates this stream 
        # tear down the experiment, unload agents, leave groups.
        for name, agent in self.aal['agents'].iteritems():
            self.teardownStream.append(UnloadAgentCall(name, **agent))
        for name, agent in self.aal['agents'].iteritems():
            timeout = agent.get('loadTimeout', self.agentLoadTimeout)
            # Use the same timeouts as setup for teardown stream
            self.teardownStream.append(
                TriggerList([
                        {'event': 'AgentUnloadDone',
                        'agent': name,
                        'nodes': self.aal['groups'][agent['group']]},
                        {'event': 'AgentUnloadDone', 'agent': name, 
                         'result': False, 'target': 'exit'},
                        {'timeout': int(timeout), 'target': 'exit'} 
                         ]))

        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(LeaveGroupCall(name, nodes))
        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(
                TriggerList([
                    {'event': 'GroupTeardownDone', 'group': name, 
                     'nodes': nodes},
                    {'event': 'GroupTeardownDone', 'result': False,
                    'target': 'exit'},
                    {'timeout': int(groupBuildTimeout), 'target': 'exit'}]))


        ##### EVENT STREAMS #####

        for key, estream in self.aal['eventstreams'].iteritems():
            newstream = Stream()
            self.streams[key] = newstream
            for event in estream:
                # The eventstream consists of triggers and events. 
                # First we process the type trigger, then event. 
                # we log errors if it is not an event or trigger.
                if event['type'] == 'event':
                    agent = self.aal['agents'][event['agent']]
                    newstream.append(EventMethodCall(agent, event))
                    if 'trigger' in event:
                        self.oeventtriggers[key].add(event['trigger'])
                        
                elif event['type'] == 'trigger':
                    triggerList = TriggerList(event['triggers'])
                    newstream.append(triggerList)
                    self.ieventtriggers[key].update(set([trigger.event 
                                                         for trigger in 
                                                         getEventTriggers(triggerList)]))
                        
                else:
                    log.warning("Skipping unknown stream entry type %s",
                                event['type'])

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

    def _resolveReferences(self, aal):
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
        if ('agents' not in aal or 'groups' not in aal or
                'eventstreams' not in aal):
            raise AALParseError('agents or groups or eventstreams not found in'
                                'AAL file. Unable to continue.')
            
        # Map outgoing triggers to respective agents
        triggerToAgentMap = {}
        for stream in aal['eventstreams']:
            for event in aal['eventstreams'][stream]:
                if event['type'] == 'event':
                    if 'trigger' in event:
                        trigger = event['trigger']
                        agent = event['agent']
                        # Store trigger to agent mapping
                        triggerToAgentMap.setdefault(trigger, agent)
                    
        # Map agents to corresponding nodes
        agentToNodesMap = {}
        for agent in aal['agents']:
            # For the agent, find the group
            if 'group' not in aal['agents'][agent]:
                raise AALParseError('No "group" found in agent'
                                    ' %s' % agent)

            group = aal['agents'][agent]['group']
            if group not in aal['groups']:
                raise AALParseError('Unable to find group %s '
                                    'in groups.' % group)

            # Got the group, find the nodes.
            nodes = set(aal['groups'][group])
            
            # Store trigger to nodes mapping
            agentToNodesMap[agent] = nodes

                                                            
        def updateTrigger(triggerData):
            triggeType = getTriggerType(triggerData)
            if triggeType in [Trigger.TIMEOUT]:
                pass
            elif triggeType in [Trigger.EVENT]:
                triggerEvent = triggerData['event']
                if triggerEvent not in triggerToAgentMap:
                    raise AALParseError('No outgoing event for trigger '
                                        '"%s"' %(triggerEvent))
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
        for stream in aal['eventstreams']:
            for event in aal['eventstreams'][stream]:
                if event['type'] == 'trigger':
                    for trigger in event['triggers']:
                        updateTrigger(trigger)                

    def __repr__(self):
        rstr = "Setup Stream\n\n" 
        rstr += str(self.setupStream)
        rstr += "\n\nEvent Streams\n\n" 
        for s in self.getStartKeys():
            rstr += s 
            rstr += "\n\n"
            rstr += str(self.streams[s])
            rstr += "\n\n"
        rstr += "Teardown Stream\n\n" 
        rstr += str(self.teardownStream)
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

    x = AAL(files=options.file, dagdisplay=True)
    print "Incoming Event triggers", x.ieventtriggers
    print "Outgoing Event triggers", x.oeventtriggers
    
    outGoingEventTriggers = set()
    for triggerSet in x.oeventtriggers.values():
        outGoingEventTriggers |= triggerSet
        
    inComingEventTriggers = set()
    for triggerSet in x.ieventtriggers.values():
        inComingEventTriggers |= triggerSet
        
    print "outGoingEventTriggers: ", outGoingEventTriggers
    print "inComingEventTriggers: ", inComingEventTriggers
    
    if inComingEventTriggers.issubset(outGoingEventTriggers):
        print 'Incoming event triggers is a subset of outgoing event triggers'
    else:
        log.error('Incoming event triggers is not a subset of outgoing event triggers')
        
    print x.__repr__()


