#!/usr/bin/env python
import logging
import threading
import yaml
import time
import sys
import Queue
import pdb
from collections import defaultdict

from parse import EventObject, TriggerList, TriggerData
from magi.orchestrator.OrchestratorDisplay import OrchestratorDisplayState
from magi.orchestrator.dagdisplay import DagDisplay

log = logging.getLogger(__name__)

class StreamIterator(object):
    """ Allows saved state navigation of a list without affecting other
    navigating the same list (just in case) """
    def __init__(self, name, stream):
        self.name = name
        self.wrapped = stream
        self.index = 0
        self._resetTimeout()

    # Get index of the current position within the eventstream 
    def getIndex(self):
        return self.index

    # Convert the message in the event stream into a string for DAG display 
    def getOneInfo(self,i):
        if isinstance(self.wrapped[i],EventObject):
            e = self.wrapped[i]
            rstr = ''
            rstr = rstr + e.method + '\n'
            if e.method == "joinGroup":
                for n in e.args['nodes']:
                    rstr = rstr + n  + ' '
            if e.groups is not None and e.groups is not "__ALL__":
                rstr = e.groups + ' '
            if e.nodes is not None and e.nodes is not "__ALL__":
                rstr = rstr + e.nodes + ' '
                print rstr
        else:
            e = self.wrapped[i]
            rstr = 'Received Trigger \n'
            print e

            for t in e:
                if t.sets:
                    if t.args['event']:
                        rstr = rstr + t.args['event'] + '\n' + str(t.sets['nodes'])
                    if t.target is not None:
                        rstr = rstr + '\n Jump to ' + t.target
            for t in e:
                print t.target, t.timeout, t.args['event']
        return rstr

    def isDone(self):
        """ true if we are done with this stream """
        log.debug('stream name: %s, check if done, current index: %s', self.getName(), str(self.index) )
        return self.index >= len(self.wrapped)

    def advance(self):
        """ Move to the next item """
        self.index += 1
        log.debug('advanced stream index to %d', self.index)

    def isEvent(self):
        """ True if current item is an event """
        return isinstance(self.wrapped[self.index], EventObject)

    def itemisEvent(self,i):
        """ True if current item is an event """
        return isinstance(self.wrapped[i], EventObject)

    def isTrigger(self):
        """ True if current item is a trigger """
        return isinstance(self.wrapped[self.index], TriggerList)

    def itemisTrigger(self,i):
        return isinstance(self.wrapped[i], TriggerList)

    def getLength(self):
        return len(self.wrapped)

    def getName(self):
        return self.name

    def getMessages(self):
        return self.wrapped[self.index].getMessages()

    def getTriggers(self):
        return self.wrapped[self.index]  # TriggerList extends from list

    def getItem(self):
        return self.wrapped[self.index]

    def resetTriggerCounts(self):
        pass 

    def getItemI(self,i):
        return self.wrapped[i]

    def _resetTimeout(self, newindex=-1):
        self.timeoutindex = newindex
        self.timeoutat = sys.maxint
        self.timeouttrigger = None

    def getTimeoutTrigger(self):
        self.getFirstTimeout()
        if time.time() > self.timeoutat:
            return self.timeouttrigger
        return None

    def getFirstTimeout(self):
        if not self.isTrigger():
            return 0
        # We are at a new location, find new value
        if self.timeoutindex != self.index:
            self._resetTimeout(self.index)
            mintimer = sys.maxint
            for t in self.wrapped[self.index]:
                if t.timeout is not None and t.timeout < mintimer:
                    self.timeoutat = time.time() + (t.timeout/1000.0)
                    self.timeouttrigger = t

        return self.timeoutat

    def getEventsWithTriggers(self, eventset):
        for i in  range(0,len(self.wrapped)):
            if self.itemisEvent(i):
                eventset[self.wrapped[i]['trigger']].update(self.getName())
                log.debug("current event set %s", eventset)
        return eventset 


    def __repr__(self):
        '''give name, index, and remaining stream events.'''
        state = 'name: %s, index: %d\n' % (self.name, self.index)
        return state + '\n'.join(['%d. %s' % (i, self.wrapped[i])
                                  for i in xrange(self.index, len(self.wrapped))])


class Orchestrator(object):
    """
        Class to parse an AAL definition, and be the event sender/receiver.
    """
    
    # if True, show current state
    show_state = False

    def __init__(self, messenger, aal, verbose=False, exitOnFailure=True, 
            useColor=True, dagdisplay=False):
        """
            Create a new Orchestrator that pushes the data from events into
            the system via messenger and collects incoming data.
             messenger - a Messenger object that is connected and ready
            events - the list of group, agents and events to send
        """
        # If true, stop the current streams
        self.stopStreams = False
        # If true, attempt to run teardownStreams when shutting down
        self.doTeardown = False

        self.messaging = messenger
        self.aal = aal
        # save triggers based on 'event' value for quicker lookup
        self.triggerCache = defaultdict(list)
        self.verbose = verbose
        self.exitOnFailure = exitOnFailure
        self.interes = set()
        self.interes.add('NOEVENT') 
        

        self.display = OrchestratorDisplayState(color=useColor)
        if dagdisplay:
            self.dagdisplay = DagDisplay()

    def runInThread(self):

        class Runner(threading.Thread):
            def __init__(self, orch):
                threading.Thread.__init__(self)
                self.orch = orch

            def run(self):
                self.orch.run()

            def stop(self):
                self.orch.stop()

        thread = Runner(self)
        thread.start()
        return thread

    def run(self):
        self.messaging.join("control", "orchestrator")
        time.sleep(0.1)  # poor man's thread.yield

        # Figure out which events to keep in cache
        # Keep only events that are being watched by other streams too 

        self.streams = [StreamIterator('initialization', self.aal.getSetupStream())]
        self.runStreams()

        self.streams = [StreamIterator(k, self.aal.getStream(k))
                        for k in self.aal.getStartKeys()]
        # Keep track of event triggers that as passed between streams 
        #pdb.set_trace()
        if self.aal.getTotalStreams() > 1:
            log.debug("TotalStreams: %d", self.aal.getTotalStreams())
            self.interes = self.aal.getInterStreamEvents()
            log.debug("Events tracked by streams %s", self.interes) 

        self.runStreams()

        if self.doTeardown:
            self.streams = [StreamIterator('exit', self.aal.getTeardownStream())]
            self.stopStreams = False
            self.runStreams()

        self.messaging.leave("control", "orchestrator")

        # TODO: some way of confirming that everything has been sent
        time.sleep(1)

    def stop(self):
        self.stopStreams = True

    def _triggerCausesExitCondition(self, trigger):
        ''' Check the trigger for an exit condition. Return True if it does,
        else False. If returns True, caller should cause stream processing
        to cease and cleanup/exit to begin.'''
        # If we're doing exitOnFailure, then exit
        # on failure
        retVal = False
        if self.exitOnFailure:
            if ('retVal' in trigger.sets and False in trigger.sets['retVal'] or
                    'retVal' in trigger.args and False is trigger.args['retVal']):
                s = ('Got a False return value from agent method. Since '
                     'exitOnFailure is True, the orchestrator will now jump'
                     ' to the exit target and unload the agents.')
                log.critical(s)
                self.display.exitOnFalse(trigger)
                retVal = True

            if ('event' in trigger.args and
                    trigger.args['event'] == 'RuntimeException'):
                log.critical('Got a runtime exception from an agent. Jumping '
                             'to exit target.')
                self.display.exitRunTimeException(trigger)
                retVal = True

        if retVal:
            self.stopStreams = True
            self.doTearDown = True

        return retVal    

    def runStreams(self):

        log.debug('running stream: \n%s', self.streams)
        while len(self.streams) > 0 and not self.stopStreams:
            try:
                updatedTrigger = None
                msg = None
                # waitfor is the minimum timeout for the current;y
                # staged event/triggers. This is usually zero unless
                # we're waiting on a trigger.
                waitfor = min([s.getFirstTimeout()
                               for s in self.streams]) - time.time()

                # if waitfor is non zero we block for s short time so we 
                # don't busy loop.
                if waitfor > 0:
                    # don't wait for longer than 3 seconds as we need
                    # to check if USR1 signal has been sent. 
                    #if self.verbose: 
                    #    self.display.waitingForMessage(3.0)
                    try: 
                        #pdb.set_trace()
                        msg = self.messaging.nextMessage(True, min(waitfor, 3.0))
                    except Queue.Empty: 
                        pass

                    if Orchestrator.show_state:
                        self.display.showTriggerDiffs(self.triggerCache, self.streams)
                        Orchestrator.show_state = False        
                    
                    if not msg:
                        continue

                else:
                    msg = self.messaging.nextMessage(False)
               
                # if self.verbose:
                #     self.display.gotMessage(msg)

                # we only act on messages to the 'control' group 
                # (of which we're a part.)
                if msg is not None:
                    if 'control' not in msg.dstgroups:
                        continue
                    else:
                        # The only messages we react to are triggers.
                        # there is no real reason for this though,
                        # if want to introduce new message types.
                        # mergeTrigger takes the new trigger, merges
                        # it to any existing cached triggers. If no
                        # cached triggers match, it appends to cache
                        # and returns what we give it.
                        updatedTrigger = self.mergeTrigger(
                            TriggerData(yaml.load(msg.data)))

                        if self._triggerCausesExitCondition(updatedTrigger):
                            continue

                        # if self.verbose: 
                        #     self.display.gotTrigger(updatedTrigger)
            
            except Queue.Empty:
                # next message will raise of no messages are there.
                # GTL TODO: so why doesn't this cause a busy loop?
                pass

            # Triggers are kept around for a brief period after they are recv'd
            # so all streams can react to them. This deletes triggers that are
            # too old.
            # self._cleanTriggerCache()

            # continue until nothing can move forward anymore
            progress = True
            numberofStreams = len(self.streams)
            while progress:
                progress = False
                finishedStreams = 0 
#                if self.streams:
#                    sn = self.streams[0].getName()
#                else:
#                    sn = "otherstream" 
#                if not ((sn == "initialization") or (sn == "exit")): 
#                     self.dagdisplay.createcallgraph() 
#                     self.dagdisplay.startToEnd(self.streams)
                # Iterate through all the streams and work on the current event 
                for streamIter in self.streams:
                    # work on the current event in each stream 
                    if streamIter.isDone():
                        # Nothing left in stream, remove the stream
                        self.display.streamEnded(streamIter)
                        finishedStreams = finishedStreams + 1
                #        self.dagdisplay.startToIndex(self.streams)
                        self.streams.remove(streamIter)
                        self._cleanTriggerCache()
                        progress = True
                    elif streamIter.isEvent():
                        # Send all the messages that the event has and move on
                        # to next
                        # GTL - after looking at the code, I think that 
                        # getMessages() always returns a list with only 
                        # one item. 
                        self.display.eventFired(streamIter)
                        for msg in streamIter.getMessages():
                            if self.verbose:
                                log.info("orch sends %s" % msg.data)
                            self.messaging.send(msg)
                        streamIter.advance()
                        progress = True
                    elif streamIter.isTrigger():
                        # getTimeoutTrigger sets internal var to 
                        # minimum time. If greater than cur time, 
                        # it returns the index of the trigger that has
                        # timed out. 
                        timedout = streamIter.getTimeoutTrigger()
                        if timedout is not None:
                            self.jumpToTarget(streamIter, timedout, True)
                            progress = True
                        # Try just matching with updated value. Remember we are 
                        # looking at a new trigger across all streams.
                        elif updatedTrigger is not None:
                            # log this below debug @ 5. Use -l all on command
                            # line to see it.
                            log.debug(' check trigger match, current stream: %s',
                                    streamIter)
                            for matchTrigger in streamIter.getTriggers():
                                if self.triggerMatch(matchTrigger,
                                                     updatedTrigger):
                                    self.display.triggerMatched(streamIter, 
                                                                matchTrigger)
                                    #pdb.set_trace()
                                    self.jumpToTarget(streamIter,
                                                      matchTrigger)
                                    updatedTrigger = None
                                    progress = True
                                    break
                        # updatedTrigger is None, take a look in the general
                        # cache for things received before
                        else:
                            for matchTrigger in streamIter.getTriggers():
                                if self.triggerMatchCache(matchTrigger):
                                    self.display.triggerMatched(streamIter, 
                                                                matchTrigger)
                                    self.jumpToTarget(streamIter, matchTrigger)
                                    progress = True
                                    break
                    else:
                        log.error("unknown object in stream")
#                if self.streams:
#                    sn = self.streams[0].getName()
#                else:
#                    sn = "otherstream" 
#                if not ((sn == "initialization") or (sn == "exit")): 
#                    self.dagdisplay.startToIndex(self.streams)
#                    self.dagdisplay.writecallgraph('events')

                if finishedStreams == numberofStreams:
                    progress = False
                    self.stopStreams = False
                    for streamIter in self.streams:
                        self.streams.remove(streamIter)


    def jumpToTarget(self, streamIter, match, isTimeout=False):
        """ Advance stream based on the target """
        log.debug("Jumping to new target: trigger firing %s, target is %s, isTimeout %s" %
                     (match.args, match.target, isTimeout))

        if match.target is None:
            streamIter.advance()
        elif match.target == 'exit':
            self.display.streamJump(streamIter, match.target)
            self.streams.remove(streamIter)
            self.stopStreams = True  # stop current streams
            self.doTeardown = True     # attempt a clean teardown before exit
        elif self.aal.hasStream(match.target):
            self.display.streamJump(streamIter, match.target)
            #pdb.set_trace()
            name = streamIter.name
            self.streams.remove(streamIter)
            self.streams.append(
                StreamIterator(name, self.aal.getStream(match.target)))
            # Before jumping make sure the cache is clean too 
            self._cleanTriggerCache()
        else:
            log.error("Couldn't find target stream %s, stopping here",
                      match.target)
            self.streams.remove(streamIter)

    def mergeTrigger(self, incoming):
        """
            Try and merge the data into our list.
            If it matches another trigger, it matches any list data, otherwise,
            it just gets added to the end of the list
            Return the value that was appended or modified.
        """
        log.debug('incoming trigger: %s' % incoming)
        event = incoming.args.get('event', 'NOEVENT')
        log.debug('trigger cache: %s' % self.triggerCache) 
        for match in self.triggerCache[event]:
            # compare two dicts, should be equal
            if match.args != incoming.args:
                log.debug('merge trigger args - match: %s, incoming %s',
                        match.args, incoming.args)
                continue
            # Found a match, merge
            log.debug('found trigger args match %s', match)
            # Here we here back from other nodes in the group that are now responding 
            # we append the set of nodes with other node info 
            for k, v in incoming.sets.iteritems():
                match.sets[k].update(v)

            # Special case: retVals are folded into a set with a single value.
            # If the incoming trigger is False, the retVal set is always
            # [False].
            # i.e. the retl' in trigger.sets and False in trigger.sets['retVal'] or$Val set is a logical AND of all retVals seen for this
            # trigger.
            if ('retVal' in incoming.sets and False in incoming.sets['retVal'] or
                             'retVal' in incoming.args and False in incoming.args['retVal']):
                if self.exitOnFailure:
                    log.critical('Got a False return value from testnode and '
                                 'exit on failure is true.')
                    log.critical('This will cause the orchestrator to exit.')
                    log.critical('The "False" data: %s' % incoming)
                match.args['retVal'] = False
            log.debug("Updated trigger in cache, now %s", match)
            return match

        # No match found, just append, do an assignment from list to set now
        log.debug("Trigger not in cache, adding to cache")
        self.triggerCache[event].append(incoming)
        log.debug('Now trigger cache: %s' % self.triggerCache) 
        return incoming

    def triggerMatchCache(self, trigger):
        """
            Check a trigger versus what we have sitting around in the cache
        """
        event = trigger.args.get('event', 'NOEVENT')
        for cacheValue in self.triggerCache[event]:
            if self.triggerMatch(trigger, cacheValue):
                return True

        # TODO: just check if the event exisits in the triggerCache
        # currently we are not checking the args or sets for the trigger
        # if there is any entry based on the trigger in the cache
        # we return true 
        eventsets = trigger.sets.get('eset')
        i=0
        if eventsets:
            for e in eventsets:
                if self.triggerCache[e]:
                    i= i + 1
            if len(eventsets) == i:
                return True 
                    
        return False

    def triggerMatch(self, local, incoming):
        """
            Compare the two triggers to see if they match, incoming must
            contain the entire set of local but can also be a superset.
            (Except for optional args.)
            @param local local trigger to match with
            @param incoming the incoming trigger from a message
            @return true if the incoming value satisfies the entire local
                trigger
        """
        log.debug("in trigger match: local: %s, incoming %s", local,incoming)
        if local is None or incoming is None:
            log.debug("match empty compare - local %s, incoming %s", 
                    local, incoming)
            return False

        if local.args != incoming.args:
            log.debug("args dict mismatch %s, %s", local.args, incoming.args)
            return False

        for key in local.sets:
            if key not in incoming.sets:
                log.debug("incoming missing set %s", key)
                return False

            if not local.sets[key].issubset(incoming.sets[key]):
                missing = local.sets[key] - incoming.sets[key]
                msg = "trigger set '%s' not complete. Waiting on " % key
                if len(missing) > 10:
                    msg += str(list(missing)[1:10]) + " ... "
                else:
                    msg += str(missing)

                log.info(msg)
                return False

        # Let the triggerdata compute possible constraint values.
        local.update(incoming)
        if incoming.constraintMatched():
            # We found a match, even given contraints.
            # We do not immediately delete ther thread
            # though, as other streams may be looking for the
            # same trigger. So we set it to self destruct in 5 seconds.
            # (This valus is checked in the main message recv loop.)
            local.selfDestructTime = time.time() + 5.0
            # Remove all events from cache  
            # once the trigger is matched and when no other stream is 
            # waiting on this trigger 
#            self._cleanTriggerCache()
            return True

        # Everything matched except the constraints.
        return False

    def _cleanTriggerCache(self):
        '''Remove all triggers that say they should be deleted.'''
        #curTime = time.time()
        #for event in self.triggerCache.iterkeys():
        #    self.triggerCache[event][:] = [t for t in self.triggerCache[event]
        #                                   if t.shouldDelete(curTime)]
        # The cache is cleared independent of time.
        #pdb.set_trace()
        # We remove all events that no other stream is interested in 
        log.debug("Events tracked by other stream %s", self.interes) 
        for event in self.triggerCache.iterkeys():
            if event not in self.interes:
                log.debug("Removing all caches events %s", event) 
                self.triggerCache[event] = list()
        log.debug("After Clean Cache: %s", self.triggerCache)
