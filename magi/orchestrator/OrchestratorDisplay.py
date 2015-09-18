
from collections import defaultdict
import logging
import pprint
from socket import gethostname
import time

from magi.db import Collection
from magi.db.Collection import DATABASE_SERVER_PORT

import parse


log = logging.getLogger(__name__)


class OrchestratorDisplayState(object):
    '''A class that displays DAG transistion and orchstrator states in concise strings to 
    stdout. The Orchestrator instance calls methods in this class at the appropriate times
    and state transitions.'''
    def __init__(self, color=True, dbHost="localhost", dbPort=DATABASE_SERVER_PORT):
        self.maxListDisplay = 5
        self.color = color
        self.waitingOn = defaultdict()
        self.collection = None
        try:
            self.collection = Collection.getCollection(agentName='orchdisplay', 
                                                       hostName=gethostname(),
                                                       dbHost=dbHost,
                                                       dbPort=dbPort)
            self.collection.remove({})
            log.info('Database collection initialized')
        except Exception, e:
            log.error('Cannot initialize database: %s' %str(e))

    def _name(self, si):
        if not si:
            return 'stream %-14s' % 'unknown'
        
        name = si if isinstance(si, str) else si.getName()
        log.debug('name: %s', name)
        if len(name) <= 14:
            return 'stream %-14s' % name[:14]
        else:
            return 'stream %-11s...' % name[:11]
    
    def waitingForMessage(self, t):
        now = self._timestamp()
        print '%s : %s : (%s) streams in wait/timeout state. Waiting at most %d seconds.' % (
            self._name('Orchestrator'), self._yellow('wait'), now, int(t))

    def gotMessage(self, msg):
        now = self._timestamp()
        print '%s : %s : (%s) got message: %s' % (
            self._name('Orchestrator'), self._green('cont'), now, str(msg))

    def gotTrigger(self, trigger):
        now = self._timestamp()
        print '%s : %s : (%s) got trigger: %s:%s:%s' % (
            self._name('Orchestrator'), self._blue('rcvd'), now, trigger.event, list(trigger.nodes), trigger.args)

    def streamEnded(self, streamIter):
        now = self._timestamp()
        print '%s : %s : (%s) complete.' % (
            self._green(self._name(streamIter)), self._green('DONE'), now)
        if self.collection:
            self.collection.insert({'stream' : self._name(streamIter),
                                    'type' : 'DONE',
                                    'msg' : 'complete'})

    def streamJump(self, streamIter, target):
        '''Call when the execution path jumps to new stream.'''
        now = self._timestamp()
        print '%s : %s : (%s) Execution path of stream has jumped to target: %s.' % (
            self._green(self._name(streamIter)), self._yellow('jump'), now, 
            target)
        if self.collection:
            self.collection.insert({'stream' : self._name(streamIter),
                                    'type' : 'jump',
                                    'msg' : 'Execution path of stream has jumped to target: %s' %(target)})

    def eventFired(self, streamIter):
        e = streamIter.next()
        to = self._minstr(e.groups)
        if not to:
            to = self._minstr(e.nodes)
       
        args = ''
        if e.args:
            args = pprint.pformat(e.args.values(), width=20)
            args = args.replace('\n', ' ')
            args = args[:20] + ' ... ' if len(args) > 20 else args
      
        trigger = getattr(e, 'trigger', None)
        trigger = '' if not trigger else ' (fires trigger: %s)' % trigger

        now = self._timestamp()
        if e.method == 'joinGroup' or e.method == 'leaveGroup': 
            print '%s : %s : (%s) %s %s --> %s %s' % (
                self._name(streamIter), self._blue('sent'), now, e.method, e.args['group'],
                to, trigger)
            if self.collection:
                self.collection.insert({'stream' : self._name(streamIter),
                                        'type' : 'sent',
                                        'msg' : '%s %s --> %s %s' %(e.method, e.args['group'], to, trigger)})
        elif e.method == 'loadAgent' or e.method == 'unloadAgent': 
            print '%s : %s : (%s) %s %s --> %s %s' % (
                self._name(streamIter), self._blue('sent'), now, e.method, e.args['name'],
                to, trigger)
            if self.collection:
                self.collection.insert({'stream' : self._name(streamIter),
                                        'type' : 'sent',
                                        'msg' : '%s %s --> %s %s' %(e.method, e.args['name'], to, trigger)})
        else:
            print '%s : %s : (%s) %s(%s) --> %s %s' % (
                self._name(streamIter), self._blue('sent'), now, e.method, self._minstr(args),
                to, trigger)
            if self.collection:
                self.collection.insert({'stream' : self._name(streamIter),
                                        'type' : 'sent',
                                        'msg' : '%s(%s) --> %s %s' %(e.method, self._minstr(args), to, trigger)})

    def triggerCompleted(self, streamIter, trigger):
        now = self._timestamp()
        if isinstance(trigger, parse.TimeoutTrigger):
            print '%s : %s : (%s) trigger completed: timeout: %s' % (
                self._name(streamIter), self._green('trig'), now, trigger.timeout)
            if self.collection:
                self.collection.insert({'stream' : self._name(streamIter),
                                        'type' : 'trig',
                                        'msg' : 'trigger completed: timeout: %s' %(trigger.timeout)})
        elif isinstance(trigger, parse.EventTrigger):
            print '%s : %s : (%s) trigger completed: %s: %s' % (
                self._name(streamIter), self._green('trig'), now, trigger.event, trigger.args)
            if self.collection:
                self.collection.insert({'stream' : self._name(streamIter),
                                        'type' : 'trig',
                                        'msg' : 'trigger completed: %s: %s' %(trigger.event, trigger.args)})
        else:
            print '%s : %s : (%s) trigger completed: %s' % (
                self._name(streamIter), self._green('trig'), now, trigger.__class__.__name__)
            if self.collection:
                self.collection.insert({'stream' : self._name(streamIter),
                                        'type' : 'trig',
                                        'msg' : 'trigger completed: %s' %(trigger.__class__.__name__)})
            
    def waitingOnTriggers(self, cache, streams):
        '''Go through all outstanding triggers and find matched in the cache. Display
        the difference between what we've seen and what we want i.e. what we are 
        still waiting on.'''
        now = self._timestamp()
        for streamItr in streams:
            if streamItr.isNextTrigger():
                for trigger in streamItr.next():
                    if isinstance(trigger, parse.TimeoutTrigger):
                        timeout = trigger.timeActivated + trigger.timeout - time.time()
                        print '%s : %s : (%s) timeout trigger will complete in %d seconds' % (
                            self._name(streamItr.name), self._red('wait'), now, timeout)
                    elif isinstance(trigger, parse.EventTrigger):
                        event = trigger.event
                        completedNodes = set()
                        for cachedTrigger in cache[event]:
                            if cachedTrigger.isEqual(trigger):
                                completedNodes = cachedTrigger.nodes
                                break
                        remainingNodes = trigger.nodes - completedNodes
                        print '%s : %s : (%s) event trigger %s : %s waiting to be received from nodes %s' % (
                            self._name(streamItr.name), self._red('wait'), now, trigger.event, trigger.args, self._minstr(remainingNodes))
                    else:
                        print '%s : %s : (%s) %s waiting to be completed' % (
                            self._name(streamItr.name), self._red('wait'), now, trigger.__class__.__name__)

    def exitOnFalse(self, trigger):
        now = self._timestamp()
        nodes = self._minstr(trigger.nodes) if trigger.nodes else 'unknown'
        event = trigger.event
        group = trigger.args.get('group', 'unknown')
        agent = trigger.args.get('agent', 'unknown')
        print '%s : %s : (%s) trigger %s returned with False on agent %s in group %s and on node(s): %s.' % (
            self._name(None), self._red('exit'), now, event, agent, group, nodes)

    def exitRunTimeException(self, trigger):
        now = self._timestamp()
        nodes = self._minstr(trigger.nodes) if trigger.nodes else 'unknown'
        func_name = trigger.args.get('func_name', 'unknown')
        filename = trigger.args.get('filename', 'unknown')
        line_num = trigger.args.get('line_num', 'unknown')
        agent = trigger.args.get('agent', 'unknown')
        error = trigger.args.get('error', 'unknown')
        print ('%s : %s : (%s) Run-time exception in agent %s on node(s) %s '
               'in method %s, line %s, in file %s. Error: %s' % (
                   self._name(None), self._red('exit'), now, agent, nodes, 
                   func_name, line_num, filename, error))

    def _timestamp(self):
        return time.ctime().split()[3]

    def _minstr(self, l):
        if not l:
            return None

        if isinstance(l, str):
            return l

        if isinstance(l, set):
            l = list(l)

        if len(l) > self.maxListDisplay:
            return ', '.join(l[:self.maxListDisplay]) + ' ... '
        else:
            return ', '.join(l)

    def _color(self, code, s):
        if self.color:
            return '\033[%d;1m%s\033[0m' % (code, s)
        else:
            return s

    def _red(self, s):
        return self._color(31, s)

    def _green(self, s):
        return self._color(32, s)

    def _yellow(self, s):
        return self._color(33, s)

    def _blue(self, s):
        return self._color(34, s)

