#!/usr/bin/env python
import logging
import threading
import yaml
import time
import Queue
from collections import defaultdict

from parse import EventObject, TriggerList, createTrigger
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
        if not self.isDone():
            if self.isNextTrigger():
                self.next().activate()
                log.debug('First item is a trigger. Activated it.')

    def getName(self):
        return self.name
    
    def getLength(self):
        return len(self.wrapped)
    
    def getIndex(self):
        """ current index within the eventstream """
        return self.index

    def isDone(self):
        """ true if we are done with this stream """
        log.debug('stream name: %s, check if done, current index: %s', self.getName(), str(self.index) )
        return self.index >= len(self.wrapped)

    def advance(self):
        """ Move to the next item """
        self.index += 1
        log.debug('advanced stream index to %d', self.index)
        if not self.isDone():
            if self.isNextTrigger():
                self.next().activate()
                log.debug('Next item is a trigger. Activated it.')

    def isEvent(self, i):
        """ If item at a given position is an event """
        return isinstance(self.wrapped[i], EventObject)

    def isNextEvent(self):
        """ If current item is an event """
        return self.isEvent(self.index)

    def isTrigger(self, i):
        """ If item at a given position is a trigger """
        return isinstance(self.wrapped[i], TriggerList)
        
    def isNextTrigger(self):
        """ If current item is a trigger """
        return self.isTrigger(self.index)

    def getItemI(self, i):
        return self.wrapped[i]
    
    def next(self):
        return self.wrapped[self.index]

    def getFirstTimeout(self):
        if self.isNextTrigger():
            return self.next().getTimeout()
        return 0
        
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
        self.doTearDown = False

        self.messaging = messenger
        self.aal = aal
        
        # save triggers based on 'event' value for quicker lookup
        self.triggerCache = defaultdict(list)
        
        self.exitOnFailure = exitOnFailure
        self.verbose = verbose

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

        log.info("Running Initialization Stream")
        self.streams = {'initialization' : 
                        StreamIterator('initialization', 
                                       self.aal.getSetupStream())}
        self.runStreams()

        log.info("Running Event Stream")
        self.streams = { k : StreamIterator(k, self.aal.getStream(k))
                        for k in self.aal.getStartKeys()}
        if self.aal.getTotalStreams() > 1:
            log.debug("TotalStreams: %d", self.aal.getTotalStreams())

        self.runStreams()

        if self.doTearDown:
            log.info("Running Exit Stream")
            self.streams = {'exit' : 
                            StreamIterator('exit', 
                                           self.aal.getTeardownStream())}
            self.stopStreams = False
            self.exitOnFailure = False
            self.runStreams()

        self.messaging.leave("control", "orchestrator")
        time.sleep(1) # TODO: some way of confirming that everything has been sent

    def stop(self, doTearDown=False):
        self.stopStreams = True
        self.doTearDown = doTearDown

    def _triggerCausesExitCondition(self, trigger):
        ''' 
            Check the trigger for an exit condition. Return True if it does,
            else False. If returns True, caller should cause stream processing
            to cease and cleanup/exit to begin.
        '''
        # If we're doing exitOnFailure, then exit on failure
        if self.exitOnFailure:
            if (trigger.args.get('result') == False):
                s = ('Got a False return value from agent method. Since '
                     'exitOnFailure is True, the orchestrator will now jump'
                     ' to the exit target and unload the agents.')
                log.critical(s)
                self.display.exitOnFalse(trigger)
                return True

            if (trigger.event == 'RuntimeException'):
                log.critical('Got a runtime exception from an agent. Jumping '
                             'to exit target.')
                self.display.exitRunTimeException(trigger)
                return True

        return False    

    def runStreams(self):

        log.debug('running stream: \n%s', self.streams)
        while len(self.streams) > 0 and not self.stopStreams:
            try:
                
                msg = None
                # minimumTimeout is the minimum timeout for the currently
                # staged event/triggers. This is usually zero unless
                # we're waiting on a trigger.
                minimumTimeout = min([s.getFirstTimeout()
                               for s in self.streams.values()]) - time.time()

                if minimumTimeout > 0:
                    # don't wait for longer than 3 seconds as we need to be 
                    # able to listen for signals
                    try: 
                        #if self.verbose: 
                        #    self.display.waitingForMessage(3.0)
                        msg = self.messaging.nextMessage(True, min(minimumTimeout, 3.0))
                    except Queue.Empty: 
                        pass

                    if not msg:
                        continue

                else:
                    # block for short time so we don't busy loop.
                    msg = self.messaging.nextMessage(True, 0.1)
               
                # if self.verbose:
                #     self.display.gotMessage(msg)

                # we only act on messages to the 'control' group 
                # (of which we're a part.)
                if msg is not None and 'control' in msg.dstgroups:
                        # The only messages we react to are triggers.
                        # there is no real reason for this though,
                        # if want to introduce new message types.
                        # cacheTrigger adds the new trigger to the cache, and
                        # returns the list of triggers corresponding to the 
                        # incoming trigger event.
                        newTrigger = createTrigger(yaml.load(msg.data))
                        self.cacheTrigger(newTrigger)
                        
                        if self._triggerCausesExitCondition(newTrigger):
                            self.stop(doTearDown=True)
                            continue
                        
                        if self.verbose:
                            self.display.gotTrigger(newTrigger)
            
            except Queue.Empty:
                pass

            if Orchestrator.show_state:
                self.display.waitingOnTriggers(self.triggerCache, self.streams.values())
                Orchestrator.show_state = False
                    
            # continue until nothing can move forward anymore
            progress = True
            while progress:
                progress = False

                for streamIter in self.streams.values():
                    # work on the current event in each stream 
                    if streamIter.isDone():
                        # Nothing left in stream, remove the stream
                        self.display.streamEnded(streamIter)
                        # self.dagdisplay.startToIndex(self.streams)
                        del self.streams[streamIter.getName()]
                        
                    elif streamIter.isNextEvent():
                        event = streamIter.next()
                        # Send all the event messages and move on.
                        # getMessages() always returns a list with only 
                        # one item. 
                        self.display.eventFired(streamIter)
                        for msg in event.getMessages():
                            if self.verbose:
                                log.info("orch sends %s" % msg.data)
                            self.messaging.send(msg)
                        
                        if event.trigger:
                            # this event has an accompanying trigger
                            # if the cache has the same trigger, it becomes
                            # stale now, and should be removed
                            self.triggerCache.pop(event.trigger, None)
                            
                        streamIter.advance()
                        progress = True
                        
                    elif streamIter.isNextTrigger():
                        triggerList = streamIter.next()
                        for trigger in triggerList:
                            if trigger.isComplete(self.triggerCache):
                                self.display.triggerCompleted(streamIter, trigger)
                                self.jumpToTarget(streamIter, trigger)
                                progress = True
                                
                    else:
                        log.error('Unknown object in stream "%s". ' \
                                    'Moving ahead.' %(streamIter.getName()))
                        log.error(streamIter.next())
                        streamIter.advance()
                        progress = True


    def jumpToTarget(self, streamIter, trigger):
        """ Advance stream based on the target """
        log.debug("Jumping to new target: trigger firing %s, target is %s" %
                     (trigger, trigger.target))

        if trigger.target is None:
            streamIter.advance()
        else:
            del self.streams[streamIter.getName()]
            if trigger.target == 'exit':
                self.display.streamJump(streamIter, trigger.target)
                self.stopStreams = True  # stop current streams
                self.doTearDown = True   # attempt a clean teardown before exit
            elif self.aal.hasStream(trigger.target):
                self.display.streamJump(streamIter, trigger.target)
                self.streams[trigger.target] = StreamIterator(trigger.target, self.aal.getStream(trigger.target))
            else:
                log.error("Couldn't find target stream %s, stopping here",
                          trigger.target)

    def cacheTrigger(self, incoming):
        """
            Try and merge the data into our list.
            If it matches another trigger, it matches any list data, otherwise,
            it just gets added to the end of the list
            Return the value that was appended or modified.
        """
        event = incoming.event
        log.debug('trigger cache: %s' % self.triggerCache) 
        merged = False
        for trigger in self.triggerCache[event]:
            if trigger.isEqual(incoming):
                trigger.merge(incoming)
                merged = True
                break
        if not merged:
            self.triggerCache[event].append(incoming)
        log.debug('Updated trigger cache: %s' % self.triggerCache) 
        
        return self.triggerCache[event]

