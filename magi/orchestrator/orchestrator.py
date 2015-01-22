#!/usr/bin/env python

import Queue
from collections import defaultdict
import copy
import logging
from socket import gethostname
import threading
import time

from magi.db import Collection
from magi.db.Collection import DATABASE_SERVER_PORT
from magi.orchestrator.OrchestratorDisplay import OrchestratorDisplayState
from magi.orchestrator.dagdisplay import DagDisplay
from magi.util import helpers
import yaml

from parse import EventObject, TriggerList, createEvent, createTrigger, Stream


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
                
        self.dbIndex = 0

    def getName(self):
        return self.name
    
    def getLength(self):
        return len(self.wrapped)
    
    def getIndex(self):
        """ current index within the eventstream """
        return self.index

    def isDone(self):
        """ true if we are done with this stream """
        #log.debug('stream name: %s, check if done, current index: %s', self.getName(), str(self.index) )
        return self.index >= len(self.wrapped)

    def advance(self):
        """ Move to the next item """
        self.index += 1
        log.debug('advanced stream index to %d', self.index)
        if not self.isDone():
            if self.isNextTrigger():
                self.next().activate()
                log.debug('Next item is a trigger. Activated it.')
                
    def recordNext(self, collection):
        if self.isDone():
            return
        if self.isNextEvent():
            eventType = "event"
            event = self.next()
            eventLabel = "Send\nAgent:%s\nMethod:%s" %(event.agent, event.method)
        elif self.isNextTrigger():
            eventType = "trigger"
            eventLabel = "Wait for \n"
            triggers = self.next();
            for trigger in triggers:
                eventLabel += "%s\n" %(trigger.toString())
        else:
            eventType = "Unknown"
            eventLabel = "N/A"
        collection.insert({'streamName' : self.getName(), 
                                'eventItr' : self.dbIndex, 
                                'eventType' : eventType,
                                'eventLabel' : eventLabel})
        self.dbIndex+=1

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
            useColor=True, dagdisplay=False, dbHost="localhost", dbPort=DATABASE_SERVER_PORT):
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
        self.activeTriggerCache = defaultdict(list)
        
        self.exitOnFailure = exitOnFailure
        self.verbose = verbose

        self.display = OrchestratorDisplayState(color=useColor)
        if dagdisplay:
            self.dagdisplay = DagDisplay()

        self.collection = None
        try:
            self.collection = Collection.getCollection('orchestrator', 
                                                       gethostname(),
                                                       dbHost,
                                                       dbPort)
            self.collection.remove({})
            self.collection.insert({'aalSvg' : self.aal.cgraph.createSvg()})
            
        except:
            log.error('Cannot initialize database')
            
        self.record = False
        self.overWriteCachedTriggers = False

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
        self.activeStreams = {'initialization' : 
                        StreamIterator('initialization', 
                                       self.aal.getSetupStream())}
        self.runStreams()

        log.info("Running Event Stream")
        self.activeStreams = { k : StreamIterator(k, self.aal.getStream(k))
                        for k in self.aal.getStartKeys()}
        if self.aal.getTotalStreams() > 1:
            log.debug("TotalStreams: %d", self.aal.getTotalStreams())

        self.record = True
        self.overWriteCachedTriggers = True
        
        self.runStreams()
        
        self.record = False
        self.overWriteCachedTriggers = False

        if self.doTearDown:
            log.info("Running Exit Stream")
            self.activeStreams = {'exit' : 
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
    
    def deleteEvent(self, streamName, event):
        '''
        '''
        functionName = self.deleteEvent
        helpers.entrylog(log, functionName, locals())
        
        try:
            eventStream = self.aal.getStream(streamName)
        except KeyError:
            log.exception("Stream %s does not exist" %(streamName))
            raise KeyError("Stream %s does not exist" %(streamName))
        
        log.info("Current event stream %s" %(eventStream))
        
        eventObject = createEvent(event, self.aal.rawAAL)
        
        try:
            objectIndex = eventStream.index(eventObject)
            eventStream.remove(eventObject)
        except:
            log.exception("Event %s does not exist" %(event))
            raise KeyError("Event %s does not exist" %(event))
            
        if streamName in self.activeStreams:
            currentIndex = self.activeStreams[streamName].index
            if objectIndex < currentIndex:
                self.activeStreams[streamName].index -= 1
        
        log.info("Modified event stream %s" %(eventStream))
        
        helpers.exitlog(log, functionName)
        
    def addEvent(self, streamName, event, index=None):
        '''
        '''
        functionName = self.addEvent
        helpers.entrylog(log, functionName, locals())

        if not self.aal.hasStream(streamName):
            log.info("Stream %s does not exist" %(streamName))
            log.info("Creating a new stream")
            self.aal[streamName] = Stream()
        
        eventStream = self.aal.getStream(streamName)
        log.info("Current event stream %s" %(eventStream))
            
        eventObject = createEvent(event, self.aal.rawAAL)
        
        if index is not None:
            eventStream.insert(index, eventObject)
            if streamName in self.activeStreams:
                currentIndex = self.activeStreams[streamName].index
                if index < currentIndex:
                    self.activeStreams[streamName].index += 1
        else:
            eventStream.append(eventObject)
                
        log.info("Modified event stream %s" %(eventStream))
        
        helpers.exitlog(log, functionName)
        
        
    def _triggerCausesExitCondition(self, trigger):
        ''' 
            Check the trigger for an exit condition. Return True if it does,
            else False. If returns True, caller should cause stream processing
            to cease and cleanup/exit to begin.
        '''
        # If we're doing exitOnFailure, then exit on failure
        if self.exitOnFailure:
            if (trigger.args.get('retVal') == False):
                log.info(trigger)
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
        
        log.debug('running stream: \n%s', self.activeStreams)
        
        for streamIter in self.activeStreams.values():
            if self.record and self.collection:
                self.collection.insert({'streamName' : streamIter.getName(), 
                                        'eventItr' : streamIter.dbIndex, 
                                        'eventType' : "streamInit",
                                        'eventLabel' : streamIter.getName()})
                streamIter.dbIndex += 1
                streamIter.recordNext(self.collection)
        
        while len(self.activeStreams) > 0 and not self.stopStreams:
            
            if Orchestrator.show_state:
                self.display.waitingOnTriggers(self.triggerCache, self.activeStreams.values())
                Orchestrator.show_state = False
                
            try:
                
                msg = None
                # minimumTimeout is the minimum timeout for the currently
                # staged event/triggers. This is usually zero unless
                # we're waiting on a trigger.
                minimumTimeout = min([s.getFirstTimeout()
                               for s in self.activeStreams.values()]) - time.time()

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
               
                log.debug(msg)

                # we only act on messages to the 'control' group 
                # (of which we're a part.)
                if msg is not None and 'control' in msg.dstgroups:
                    
                    if self.verbose:
                        self.display.gotMessage(msg)
                    
                    msgData = yaml.load(msg.data)
                    
                    
                    if 'method' in msgData:
                        # A method call
                        log.debug("Method Call")
                        self.doMessageAction(msgData)
                    else:
                        # Otherwise, trigger
                        log.debug("Incoming Trigger")
                        try:
                            newTriggerData = msgData
                            newTriggerData.setdefault('nodes', msg.src)
                            #newTriggerData['timestamp'] = time.time()
                            newTrigger = createTrigger(newTriggerData)
                            
                            # add the received trigger to the cache
                            self.cacheTrigger(newTrigger)
                            
                            # check if any exception
                            if self._triggerCausesExitCondition(newTrigger):
                                self.stop(doTearDown=True)
                                continue
                            
                            if self.verbose:
                                self.display.gotTrigger(newTrigger)
                        except:
                            # Not a valid trigger
                            log.exception('Not a valid message')
                            pass
                        
            except Queue.Empty:
                pass

            # continue until nothing can move forward anymore
            progress = True
            while progress:
                progress = False

                for streamIter in self.activeStreams.values():
                    # work on the current event in each stream 
                    if streamIter.isDone():
                        # Nothing left in stream, remove the stream
                        self.display.streamEnded(streamIter)
                        # self.dagdisplay.startToIndex(self.activeStreams)
                        del self.activeStreams[streamIter.getName()]
                        
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
                        
                        if self.overWriteCachedTriggers and event.trigger:
                            # this event has an accompanying trigger
                            # if the cache has the same trigger, it becomes
                            # stale now, and should be removed
                            #self.triggerCache.pop(event.trigger, None)
                            self.invalidateTriggers(event.trigger, None)
                            
                        #Collecting outgoing event data into the database    
#                         if self.collection:
#                             self.collection.insert({'type' : 'event', 
#                                                     'streamName' : streamIter.getName(), 
#                                                     'method' : event.method,
#                                                     'args' : event.args,
#                                                     'trigger' : event.trigger, 
#                                                     'groups' : event.groups, 
#                                                     'nodes' : event.nodes,
#                                                     'docks' : event.docks})
            
                        streamIter.advance()
                        if self.record and self.collection:
                            streamIter.recordNext(self.collection)
                        progress = True
                        
                    elif streamIter.isNextTrigger():
                        triggerList = streamIter.next()
                        for trigger in triggerList:
                            if trigger.isComplete(self.activeTriggerCache):
                                self.display.triggerCompleted(streamIter, trigger)
                                if self.record and self.collection:
                                    self.collection.insert({'streamName' : streamIter.getName(), 
                                                            'eventItr' : streamIter.dbIndex, 
                                                            'eventType' : "triggerComplete",
                                                            'eventLabel' : "Trigger Complete\n%s" %(trigger.toString())})
                                    streamIter.dbIndex += 1
                                self.jumpToTarget(streamIter, trigger)
                                progress = True
                                break
                                
                    else:
                        log.error('Unknown object in stream "%s". ' \
                                    'Moving ahead.' %(streamIter.getName()))
                        log.error(streamIter.next())
                        streamIter.advance()
                        if self.record and self.collection:
                            streamIter.recordNext(self.collection)
                        progress = True


    def jumpToTarget(self, streamIter, trigger):
        """ Advance stream based on the target """
        log.debug("Jumping to new target: trigger firing %s, target is %s" %
                     (trigger, trigger.target))
        if trigger.target is None:
            streamIter.advance()
            if self.record and self.collection:
                streamIter.recordNext(self.collection)
        else:
            del self.activeStreams[streamIter.getName()]
            if trigger.target == 'exit':
                self.display.streamJump(streamIter, trigger.target)
                self.stopStreams = True  # stop current streams
                self.doTearDown = True   # attempt a clean teardown before exit
                if self.record and self.collection:
                    self.collection.insert({'streamName' : streamIter.getName(), 
                                    'eventItr' : streamIter.dbIndex, 
                                    'eventType' : "exit",
                                    'eventLabel' : "Exit"})
            elif self.aal.hasStream(trigger.target):
                self.display.streamJump(streamIter, trigger.target)
                self.activeStreams[trigger.target] = StreamIterator(trigger.target, self.aal.getStream(trigger.target))
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
        #log.debug('trigger cache: %s' % self.triggerCache) 
        
        #Collecting incoming trigger data into the database
    #         if self.collection:
    #             self.collection.insert({'type' : 'trigger',
    #                                     'event' : incoming.event,
    #                                     'nodes' : list(incoming.nodes), 
    #                                     'args' : incoming.args})
        
        # Invalidate existing triggers that the new trigger overrides
        if self.overWriteCachedTriggers:
            self.invalidateTriggers(incoming.event, incoming.nodes)
        
        for node in incoming.nodes:
            nodeTrigger = copy.copy(incoming)
            nodeTrigger.nodes = set([node])
            nodeTrigger.activate()
            self.triggerCache[nodeTrigger.event].append(nodeTrigger)
            self.activeTriggerCache[nodeTrigger.event].append(nodeTrigger)
        
        #log.debug('Updated trigger cache: %s' % self.triggerCache) 
        
        return self.activeTriggerCache[incoming.event]

    def invalidateTriggers(self, triggerEvent, nodes=None):
        functionName = self.invalidateTriggers.__name__
        helpers.entrylog(log, functionName, locals())
        nodes = helpers.toSet(nodes)
        activeTriggers = self.activeTriggerCache[triggerEvent]
        for trigger in activeTriggers:
            if (not nodes) or (trigger.nodes.issubset(nodes)):
                log.debug("Deactivating trigger: %s" %(trigger))
                trigger.deActivate()
                activeTriggers.remove(trigger)
                
    def doMessageAction(self, msgData):
        """
            The function takes a message, and demuxxes it. Based on the content of the message it 
            may take a number of actions. That number is currently one: invoke dispatchCall
            which calls a function on "this" object whatever it is. 
        """
        
        log.info("In doMessageAction %s", str(msgData))
            
        try: 
            method = msgData['method']
            args = msgData['args']
            meth = getattr(self, method)
            retVal = meth(**args)
        except Exception, e:
                log.error("Exception while trying to execute method")
                log.error("Sending back a RunTimeException event. This may cause the receiver to exit.")
                self.messaging.trigger(event='RuntimeException', func_name=method, agent='orchestrator', 
                                       nodes=[self.messaging.name], error=str(e))
                return
            
        if 'trigger' in msgData:
            if not isinstance(retVal, dict):
                if retVal is None:
                    retVal = True
                retVal = {'result' : retVal}
            args = retVal
            args['nodes'] = self.messaging.name
            self.messaging.trigger(event=msgData['trigger'], **args)
                    
    
