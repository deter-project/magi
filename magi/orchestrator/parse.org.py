import logging
import yaml
import sys
import cStringIO
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
            if isinstance(v, list):
                self.sets[k] = set(v)
            else:
                self.args[k] = v

    def update(self, incomingTrigData):
        '''
            Update trigger constraints. Should only be called with
            matching data.
        '''
        self.count[1] += 1

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

    def __repr__(self):
        return "TriggerData args: %s, sets: %s" % (self.args, self.sets)


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
        return '%s(%s) triggers --> %s' % (self.method, self.args, self.trigger)

class EventMethodCall(BaseMethodCall):
    """ MethodCall class for sending regular method calls from AAL events """

    def __init__(self, aalagent, aalevent):
        # trigger is optional
        trigger = None if 'trigger' not in aalevent else aalevent['trigger']
        BaseMethodCall.__init__(self, groups=aalagent['group'],
                                docks=aalagent['dock'],
                                method=aalevent['method'],
                                args=aalevent['args'], trigger=trigger)


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

    def __init__(self, files=None, data=None, groupBuildTimeout=20000):
        """
            Create a new AAL object using either files or a
            string object (data).
        """
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
            log.critical('Error reading event AAL files.')
            sys.exit(1)

        self.aal = yaml.load(yaml_file.getvalue())
        self.setupStream = list()
        self.teardownStream = list()
        self.streams = dict()

        # The AAL extra-YAML references
        self._resolveReferences(self.aal)

        # sanity check.
        for k in ['streamstarts', 'agents', 'groups', 'eventstreams']:
            if not k in self.aal.keys():
                log.critical('missing required key in AAL: %s', k)
                sys.exit(1)

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
                     'name': name,
                     'nodes': self.aal['groups'][agent['group']]},
                    {'event': 'AgentLoadDone', 'name': name, 'retVal': False,
                     'target': 'exit'}]))

        # tear down the experiment, unload agents, leave groups.
        for name, agent in self.aal['agents'].iteritems():
            self.teardownStream.append(UnloadAgentCall(name, **agent))
        for name, agent in self.aal['agents'].iteritems():
            # unload timeout is less important so a default is probably OK.
            self.teardownStream.append(
                TriggerList([{'timeout': 200000, 'event': 'AgentUnloadDone',
                              'name': name,
                              'nodes': self.aal['groups'][agent['group']]}]))

        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(LeaveGroupCall(name, nodes))
        for name, nodes in self.aal['groups'].iteritems():
            self.teardownStream.append(
                TriggerList([{'timeout': 20000, 'event': 'GroupTeardownDone',
                              'group': name, 'nodes': nodes}]))

        for key, estream in self.aal['eventstreams'].iteritems():
            newstream = list()
            self.streams[key] = newstream
            for event in estream:
                if event['type'] == 'trigger':
                    newstream.append(TriggerList(event['triggers']))
                elif event['type'] == 'event':
                    agent = self.aal['agents'][event['agent']]
                    newstream.append(EventMethodCall(agent, event))
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
