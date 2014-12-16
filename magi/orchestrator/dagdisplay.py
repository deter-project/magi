import pprint
import time
import logging
from collections import defaultdict
from parse import TriggerList 

log = logging.getLogger(__name__)


class DagDisplay(object):
    '''A class that displays DAG transition and orchestrator states as a transition 
    graph to create a image file.
    The Orchestrator instance calls methods in this class at the appropriate times
    and state transitions.'''
    def __init__(self):
        try:
            global pydot
            import pydot
        except ImportError:
            print "Could not import pydot. Display Failed"
        self.maxListDisplay = 3
        self.waitingOn = defaultdict() 
        self.count = 0
        self.createcallgraph()

    def createcallgraph(self):
        self.callgraph = pydot.Dot(graph_type ='digraph', fontname="verdana")
        self.keydict = dict()

    def startToIndex(self,streams):
        for si in streams:
            streamName= si.getName()
            for i in range (0, si.getIndex()):
                print '* %s', si.getItemI(i)

            self.keydict[streamName] =  pydot.Cluster(streamName, label=streamName)
            # First node in the graph is Start Experiment in case of the 
            # initialization else it is setup"
            if streamName == "initialization":
                prev = 'Start'
            else:
                prev = 'Setup' 
            if streamName == "exit":
                prev = 'TearDown' 
            self.callgraph.add_node(pydot.Node(prev,
                            style="filled", fillcolor="green"))
            for i in range (0,si.getLength()):
                iname = streamName + str(i)
                if i < si.getIndex():
                    self.keydict[streamName].add_node(pydot.Node(iname,
                            label=self.eventString(si,i), 
                            style="filled", fillcolor="green"))
                else:
                    self.keydict[streamName].add_node(pydot.Node(iname,
                            label=self.eventString(si,i), 
                            style="filled", fillcolor="white"))
                self.callgraph.add_edge(pydot.Edge(prev,iname))
                prev = iname 

            self.callgraph.add_subgraph(self.keydict[streamName])

    def startToEnd(self,streams):
        for si in streams:
            streamName= si.getName()
            print streamName 
            for i in range (0, si.getLength()):
                print si.getItemI(i)


    def writecallgraph(self,prefix):
        self.count = self.count+1
        fname = prefix + str(self.count) + '.png'
        self.callgraph.write_png(fname)

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

    def triggerString(self, tlist):
        rstr = tlist.__repr__()
        for t in tlist:
            print t 
            if t.sets:
                if t.args['event']:
                    rstr = 'trigger: %s ' % (
                            t.args['event'])
                    name = getattr(t.args, 'name', None)
                    print name 
                    if name:
                        rstr = rstr + '%s ' % (t.args['name'])
                    group = getattr(t.args, 'group', None)
                    print group 
                    if group:
                        rstr = rstr + '%s ' % (t.args['group'])
        return rstr 

    def streamEnded(self, streamIter):
        now = self._timestamp()
        print '%s : %s : (%s) complete.' % (
            self._green(self._name(streamIter)), self._green('DONE'), now)

    def streamJump(self, streamIter, target):
        '''Call when the execution path jumps to new stream.'''
        now = self._timestamp()
        print '%s : %s : (%s) Execution path of stream has jumped to target: %s.' % (
            self._green(self._name(streamIter)), self._yellow('jump'), now, 
            target)

    def eventString(self, streamIter,i):
        e = streamIter.getItemI(i)
        print "eventstring: %s", e
        if isinstance(e,TriggerList):
            return self.triggerString(e)
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

        if e.method == 'joinGroup' or e.method == 'leaveGroup': 
            return '%s %s --> %s %s' % (
                e.method, e.args['group'],
                to, trigger)
        elif e.method == 'loadAgent' or e.method == 'unloadAgent': 
            return '%s %s --> %s %s' % (
                e.method, e.args['name'],
                to, trigger)
        else:
            return '%s(%s) --> %s %s' % (
                e.method, self._minstr(args),
                to, trigger)

    def showTriggerDiffs(self, cache, streams):
        '''Go through all outstanding triggers and find matched in the cache. Display
        the difference between what we've seen and what we want i.e. what we are 
        still waiting on.'''
        now = self._timestamp()
        for si in streams:
            if si.isNextTrigger():
                for aal_tr in si.getTriggers():
                    event = aal_tr.args.get('event', 'NOEVENT')
                    timeout = aal_tr.args.get('timeout', None)
                    timeout = '' if not timeout else ' timeout: %d' % aal_tr.args.timeout
                    if not len(aal_tr.sets):
                        print '%s : %s : (%s) trigger %s waiting for anything %s' % (
                            self._name(si.name), self._red('wait'), now, event, timeout)
                    else:
                        diff = set()
                        for key in aal_tr.sets:
                            if not event in cache:
                                # no cached instances, show it all
                                diff = aal_tr.sets[key]
                            else:
                                for cache_tr in cache[event]:
                                    diff.update(cache_tr.sets[key] - aal_tr.sets[key])
                                
                            print '%s : %s : (%s) trigger %s waiting on %s: %s %s' % (
                                self._name(si.name), self._red('wait'), now, event, key, 
                                self._minstr(diff), timeout)

    def waitingOnTriggers(self, streamIter):
        now = self._timestamp()
        for td in streamIter.getTriggers():
            timeout = 0 if not 'timeout' in td.args else td.args['timeout']
            nodes = self._minstr(td.sets['nodes']) if 'nodes' in td.sets else 'unknown'
            print '%s : %s : (%s) trigger %s, timeout: %d, nodes: %s' % (
                self._name(streamIter.name), self._red('wait'), now, td.args['event'], 
                timeout, nodes)

    def triggerMatched(self, streamIter, trigger):
        now = self._timestamp()
        group = ' ' if not 'group' in trigger.args else ' ' + trigger.args['group'] + ' '
        name = ' ' if not 'name' in trigger.args else ' ' + trigger.args['name'] + ' '
        print '%s : %s : (%s) trigger %s%s%scomplete.' % (
            self._name(streamIter), self._green('done'), now,
            trigger.args['event'], group, name)

    def exitOnFalse(self, td):
        now = self._timestamp()
        group = self._minstr(td.args['group']) if 'group' in td.args else 'unknown'
        nodes = self._minstr(td.sets['nodes']) if 'nodes' in td.sets else 'unknown'
        method = td.args['event'] if 'event' in td.args else 'unknown'
        name = td.args['name'] if 'name' in td.args else 'unknown'
        print '%s : %s : (%s) method %s returned False on agent %s in group %s and on node(s): %s.' % (
            self._name(None), self._red('exit'), now, method, name, group, nodes)

    def exitRunTimeException(self, td):
        now = self._timestamp()
        nodes = self._minstr(td.sets['nodes']) if 'nodes' in td.sets else 'unknown'
        func_name = td.args['func_name'] if 'func_name' in td.args else 'unknown'
        filename = td.args['filename'] if 'filename' in td.args else 'unknown'
        line_num = td.args['line_num'] if 'line_num' in td.args else 'unknown'
        agent = td.args['agent'] if 'agent' in td.args else 'unknown'
        error = td.args['error'] if 'error' in td.args else 'unknown'
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
