import logging
import yaml
import sys
import cStringIO
import optparse
from controlflow import ControlGraph  
from collections import defaultdict
from magi.messaging.api import MAGIMessage


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
        self.esets = defaultdict(dict)
        self.count = [1, 0]  # default is always to look for a single match

        # handle 'meta' constraint data
        if 'count' in datadict:
            self.count = [int(datadict['count']), 0]
            del datadict['count']
        for k, v in datadict.iteritems():
            log.debug("unpacking incoming trigger k: %s, v: %s", k,v)
            if isinstance(v, list):
                #  Here  we need to check if the value v is a dictionary 
                for ll in v:
                    if isinstance(ll,dict):
                        self.esets[ll['event']] = TriggerData(ll) 
                    else:
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
        log.debug("esets: %s", self.esets) 
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
        ## Todo: current not checking for count 
        return True

   # def shouldDelete(self, curTime):
   #     if self.selfDestructTime is None:
   #         return False   # we don't care about self destruct
   #     else:
   #         return curTime > self.selfDestructTime


    def getEsets(self):
        return self.esets

    def getSets(self):
        return self.sets

    def getArgs(self):
        return self.args

    def getCount(self):
        return self.count[0]

    def __repr__(self):
        if self.sets:
            setstr = self.sets 
        else:
            setstr = "None"

        # esets is a dictionary with events as keys 
        # the triggerData is just the value within the dict entry
        if self.esets:
            esetstr = ""
            for e in self.esets:
                esetstr = esetstr + "\n\t\t" + e + ":" + TriggerData.__repr__(self.esets[e])
        else:
            esetstr = "None" 

        return "TriggerData count: %s, args: %s, sets: %s, esets: %s" % (self.count, self.args, setstr, esetstr)


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

    def getSets(self):
        return TriggerData.getSets(self) 

    def getArgs(self):
        return TriggerData.getArgs(self)

    def getCount(self):
        return TriggerData.getCount(self)

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
                    print e 
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

        # TODO: currently the startup stream is always setup for an AAL 
        # Later the experiment may or may not have a startup phase 
        self.startup = True 
        
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
        self.ieventtriggers = defaultdict(set) 
        self.oeventtriggers = defaultdict(set) 

        # The AAL extra-YAML references
        self._resolveReferences(self.aal)

        # Sanity Check: does the AAL have the following directives. 
        # if not, log that they are missing but continue 
        for k in ['streamstarts', 'agents', 'groups', 'eventstreams']:
            if not k in self.aal.keys():
                log.critical('missing required key in AAL: %s', k)

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

            # Add event call for load Agents 
            for name, agent in self.aal['agents'].iteritems():
                self.setupStream.append(LoadAgentCall(name, **agent))

            # Add triggers to ensure  the agents are loaded correctly 
            for name, agent in self.aal['agents'].iteritems():
                timeout = (200000 if not 'loadTimeout' in agent
                           else agent['loadTimeout'])
                self.setupStream.append(
                    TriggerList([
                        {'event': 'AgentLoadDone',
                        'agent': name,
                        'nodes': self.aal['groups'][agent['group']]},
                        {'event': 'AgentLoadDone', 'agent': name, 'retVal': False,
                        'target': 'exit'},
                        {'timeout': int(timeout), 'target': 'exit'} 
                         ]))

        # We always define a teardown stream as jumping to target exit 
        # activates this stream 
        # tear down the experiment, unload agents, leave groups.
        for name, agent in self.aal['agents'].iteritems():
            self.teardownStream.append(UnloadAgentCall(name, **agent))
        for name, agent in self.aal['agents'].iteritems():
            # Use the same timeouts as setup for teardown stream
            self.teardownStream.append(
                TriggerList([
                        {'event': 'AgentUnloadDone',
                        'agent': name,
                        'nodes': self.aal['groups'][agent['group']]},
                        {'event': 'AgentUnloadDone', 'agent': name, 'retVal': False,
                        'target': 'exit'},
                        {'timeout': int(timeout), 'target': 'exit'} 
                         ]))

        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(LeaveGroupCall(name, nodes))
        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(
                TriggerList([
                    {'event': 'GroupTeardownDone', 'group': name, 'nodes': nodes},
                    {'event': 'GroupTeardownDone', 'retVal': False,
                    'target': 'exit'},
                    {'timeout': int(groupBuildTimeout), 'target': 'exit'}]))

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
                    t = TriggerList(event['triggers'])

                    feset = t.getEsets()
                    if feset:
                        for k in feset:
                            self.ieventtriggers[key].add(k)

                    if dagdisplay:
                        self.cgraph.addTrigger(key,t)

                    newstream.append(t)

                elif event['type'] == 'event':
                    agent = self.aal['agents'][event['agent']]
                    newstream.append(EventMethodCall(agent, event))
                    if 'trigger' in event:
                        self.oeventtriggers[key].add(event['trigger'])
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
        log.debug("ieventtriggers: %s", self.ieventtriggers)
        for k in self.ieventtriggers:
            if len(ies) == 0:
                ies = self.ieventtriggers[k]
            else:
                ies = ies.union(self.ieventtriggers[k])
        
        log.debug("oeventtriggers: %s", self.oeventtriggers)
        for k in self.oeventtriggers:
            if len(ies) == 0:
                ies = self.oeventtriggers[k]
            else:
                ies = ies.union(self.oeventtriggers[k])

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



if __name__ == "__main__":
    optparser = optparse.OptionParser()
    optparser.add_option("-f", "--file", dest="file", help="AAL Events file", default=[], action="append")
    (options, args) = optparser.parse_args()

    x = AAL(files=options.file, dagdisplay=True)
    print "Incoming Event triggers", x.ieventtriggers
    print "Outgoing Event triggers", x.oeventtriggers
    #print x.__repr__()


