#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import Queue
import logging
import random
import subprocess
import sys
import threading
import time

from magi.messaging.magimessage import MAGIMessage
from magi.testbed import testbed
from magi.util.calls import doMessageAction
from magi.util.execl import spawn, execAndRead
from magi.util.distributions import *
from magi.util import database

log = logging.getLogger(__name__)

"""
    This file provides a series of base agents that real
    agent can derive from to provide common behaviour
"""


def agentmethod(*dargs, **dkwargs):
    """
        Let user decorate functions that are intended to be called by dispatch.
        Note, this is a documentation marker only, nothing else is done with
        it. One potential idea is to extract the IDL from these decorators
        though it would be python only.
    """
    # reword obsure error message for other decorator format call
    if len(dargs) == 1:
        raise TypeError("agent method decorator must be called with at"
                        "least parenthesis")

    def decorate(f):
        f._agentmethod = True
        f._agentargs = dkwargs
        return f

    return decorate


class Agent(object):
    """
        Provides the implementation of the setConfiguration which is used
        to set variables as defined in the agent's IDL. Also provides a
        configuration confirmation method for checking the validity of the
        configuration that was set.
    """
    def __init__(self):
        self.done = False
        self.messenger = None
        self.docklist = set()
        self.name = None
        self.hostname = None
        
    @agentmethod()
    def setConfiguration(self, msg, **kwargs):
        '''
        Allows an external entity to set internal class variables. Derived
        classes can override this if they want, but probably want to
        override confirmConfiguration() instead.
        '''
        for k, v in kwargs.iteritems():
            try:
                setattr(self, k, v)
            except Exception, e:
                log.error("Failed to set agent variable %s: %s", k, e)

        return self.confirmConfiguration()

    @agentmethod()
    def confirmConfiguration(self):
        '''
        Called after configuration is set. The derived classes should
        overload this this if they need to check configuration details.
        They should return True or False depending if the configuration is
        acceptable or not. Default is just to return True.
        '''
        return True
    
    @agentmethod()
    def stop(self, msg):
        """ Called by daemon to inform the agent that it should shutdown """
        log.warning('Got agent unload message. Shutting down.')
        for dock in self.docklist.copy():
            self.messenger.unlistenDock(dock)
        # 9/14 Changed testbed.nodename to self.hostname to support desktop daemons  
        self.messenger.trigger(event='AgentUnloadDone', agent=self.name, nodes=[self.hostname])
        self.done = True
        self.messenger.poisinPill()
        log.info('Unload Complete.')


class DispatchAgent(Agent):
    """
        Provides dispatch code for an agent that only responds/reacts
        to incoming messages, synchronously.
    """
    def __init__(self):
        log.debug('In init of the root DispatchAgent')
        Agent.__init__(self)

    def run(self):
        """ Called by daemon in the agent's thread to perform
        the thread main"""
        log.info('In run of the root DispatchAgent')
        while not self.done:
            try:
                msg = self.messenger.next(True)
                if isinstance(msg, MAGIMessage):
                    doMessageAction(self, msg, self.messenger)
            except Queue.Empty:
                pass

        
class NonBlockingDispatchAgent(Agent):
    """
        Provides dispatch code for an agent that only responds/reacts
        to incoming messages, asynchronously.
    """
    def __init__(self):
        log.debug('In init of the root NonBlockingDispatchAgent')
        Agent.__init__(self)

    def run(self):
        """ Called by daemon in the agent's thread to perform
        the thread main"""
        log.debug('In run of the root NonBlockingDispatchAgent')
        while not self.done:
            try:
                msg = self.messenger.next(True)
                if isinstance(msg, MAGIMessage):
                    thr = threading.Thread(target=doMessageAction, args=(self, msg, self.messenger))
                    thr.start()
            except Queue.Empty:
                pass


class ReportingDispatchAgent(DispatchAgent):
    """
        Provides code for an agent that accepts incoming requests as well as
        periodic reports
    """
    def __init__(self):
        log.debug('In init of the root ReportingDispatchAgent')
        DispatchAgent.__init__(self)

    def run(self):
        """
            Called by daemon in the agent's thread to perform the thread main
        """
        t1 = threading.Thread(target=self.runPeriodic)
        t1.start()
        DispatchAgent.run(self)
                
    def runPeriodic(self):
        nextreport = 0
        while not self.done:
            now = time.time()
            if now >= nextreport:
                nextreport = self.periodic(now) + time.time()
            else:
                time.sleep(nextreport - now)

    def periodic(self, now):
        """
            Called when its time to report again.
            Expects the method to return the number of seconds until
            this method should be called again
        """
        raise Exception("subclasses must implement the periodic method")


class SharedServer(DispatchAgent):
    """
        Provides interface for starting and stopping a shared instance
        of a server process
    """
    def __init__(self):
        DispatchAgent.__init__(self)
        self.hosts = set()

    @agentmethod()
    def startServer(self, msg):
        """
            Request that the server be started for this agent group.  If
            The server is already running, it simply increments the
            usage counter.
        """
        retVal = True
        if len(self.hosts) == 0:
            log.info("Starting server")
            retVal = self.runserver()
            
        self.hosts.add(msg.src)

        return retVal

    @agentmethod()
    def stopServer(self, msg):
        """
            Notify the server that the source node no longer needs it.  If
            this is the last node using it, stop the server.
        """
        retVal = True
        self.hosts.remove(msg.src)
        if len(self.hosts) == 0:
            log.info("Stopping server")
            retVal = self.terminateserver()

        return retVal


class TrafficClientAgent(Agent):
    """
        Provides a base for traffic clients that use the traffic.idl interface.
        Rather than just dispatching in the run loop, it uses that loop
        to perform waiting period for the next execution of a client.
        Subclasses want to implement getCmd(). The command given will be
        spawned on the client.
    """

    def __init__(self):
        Agent.__init__(self)
        self.subpids = list()
        # TODO: Replace hardcoded value with MAGILOG 
        self.logfile = '/var/log/magi/%s_%s.log' % (self.__class__.__name__, time.strftime("%Y-%m-%d_%H:%M:%S"))
        self.servers = []
        self.interval = "1"
        self.stopClient(None)
        
    def run(self):
        """
        Called by daemon in the agent's thread to perform the thread main
        """
        if database.isDBEnabled:
            self.collection = database.getCollection(self.name)
            
        while not self.done:
            try:
                msg = self.messenger.next(True, max(self.nextrun - time.time(), 0))
                if isinstance(msg, MAGIMessage):
                    doMessageAction(self, msg, self.messenger)

            except Queue.Empty:
                pass

            if not self.running or time.time() < self.nextrun:
                continue  # message received, not yet time to launch, reloop
            
            self.nextrun = time.time() + eval(self.interval)
            
            try:
                # TODO: Check memory here to avoid overload?
                self.oneClient()
            except Exception:
                log.error("error in client process", exc_info=1)

    def stop(self, msg):
        """ Called by daemon to inform the agent that it should shutdown """
        self.running = False
        Agent.stop(self, msg)

    def oneClient(self):
        """ Called when the next client should fire (after interval time) """
        if len(self.servers) < 1:
            log.warning("no servers to contact, nothing to do")
            return
        fp = open(self.logfile, 'a')
        dst = self.servers[random.randint(0, len(self.servers) - 1)]
        try:
            (output, err) = execAndRead(self.getCmd(dst))
            fp.write(str(time.time()) + "\t" + output)
            if self.collection:
                self.collection.insert({"result" : output, "error" : err})
        except OSError, e:
            log.error("can't execute command: %s", e)
            fp.close()

    def getCmd(self, dst):
        """
            To be implemented by subclasses, get the cmd for the next
            client to run with a given destination
        """
        log.error("Subclass did not implement getCmd(self, dst), we have "
                  "nothing to do")
        return None

    @agentmethod()
    def startClient(self, msg):
        """ Implements startClient from idl """
        self.running = True
        self.nextrun = time.time() + eval(self.interval)
        return True

    @agentmethod()
    def stopClient(self, msg):
        """ Implements stopClient from idl """
        self.running = False
        self.nextrun = sys.maxint
        return True


class ProbabilisticTrafficClientAgent(TrafficClientAgent):
    '''
        Provides the same service as TrafficAgent, but getCmd is called
        only when the configured probablity function evaluates to a 
        non-zero value.
    '''
    def __init__(self):
        TrafficClientAgent.__init__(self)
        self.probFunction = "minmax(0,100) > 50.0"

    # just "overload" oneClient with call to prob function and only
    # call getCmd if # prob function evaluates to True
    def oneClient(self):
        """ Called when the next client should fire (after interval time) """
        if len(self.servers) < 1:
            log.warning("no servers to contact, nothing to do")
            return

        fp = open(self.logfile, 'a')
        dst = self.servers[random.randint(0, len(self.servers) - 1)]
        if eval(self.probFunction):
            try:
                spawn(self.getCmd(dst), stdin=None, stdout=fp,
                      stderr=subprocess.STDOUT)
            except OSError, e:
                log.error("can't execute command: %s", e)
                fp.close()


class ConnectedTrafficClientsAgent(Agent):
    """
        Provides a base for an agent which controls a set of agents
        which have standing connections to, and traffic between, a set
        of servers. connect() and disconnect() are called periodically
        when a given client should connect or disconnect to a
        given server. generateTraffic() is called when the given client
        should generate traffic between itself and the server it is
        connected to. The sequence of calls
        is: [period], connect(), [period], generateTraffic(), [period],
        generateTraffic(), ..., disconnect(). This sequence may be repeated.

        Derived classes should implement connect(), disconnect(), and
        generateTraffic().
    """
    def __init__(self):
        Agent.__init__(self)
        self.connectionTable = None
        self.stopTraffic(None)

        # externally set vars.
        # size, interval, and duration are all of 'think' type. See
        # distributions.py for valid values.
        self.servers = []
        self.trafficSize = 'minMax(1,1024)'         # traffic size in bytes
        self.trafficInterval = 'minMax(2,6)'        # time between bursts.
        self.connectionInterval = 'minMax(80,100)'  # time between connections.
        self.connectionDuration = 'minMax(10,30)'   # duration of a connection.

    def run(self):
        """ Called by daemon in the agent's thread to perform
        the thread main """

        while not self.done:
            try:
                next_time = self.nextrun - time.time()
                next_time = next_time if next_time > 0 else 0
                msg = self.messenger.next(True, next_time)
                if isinstance(msg, MAGIMessage):
                    doMessageAction(self, msg, self.messenger)

            except Queue.Empty:
                pass

            if not self.running:
                continue  # message received, not yet time to launch, reloop

            now = time.time()
            for server, [connected, connTime, trafTime, connDuration] in self.connectionTable.items():
                log.debug("checking if it's time for action")
                if connTime < now and not connected:
                    # connect
                    log.info("client %s connecting to server %s" %
                             (testbed.nodename, server))
                    self.connect(server)
                    self.connectionTable[server][0] = True
                    self._setNextRunTime()
                elif connTime + connDuration < now:
                    # disconnect and reset next connection/traffic times.
                    log.info("client %s disconnecting from server %s" %
                             (testbed.nodename, server))
                    self.disconnect(server)
                    self._setConnectionTrafficTime(server)
                elif trafTime < now:
                    # generate X bytes of traffic and reset for the next
                    # traffic generation time.
                    size = eval(self.trafficSize)
                    log.info("%s generating %s bytes of IRC traffic" %
                             (testbed.nodename, str(size)))
                    self.generateTraffic(server, int(size))
                    nextTrafStart = trafTime + eval(self.trafficInterval)
                    self.connectionTable[server][1] = nextTrafStart
                    self._setNextRunTime()

    def stop(self, msg):
        """ Called by daemon to inform the agent that it should shutdown """
        self.running = False
        self.connectionTable = {}
        Agent.stop(self, msg)

    @agentmethod()
    def startTraffic(self, msg):
        """ Implements startClient from idl """
        # Initialize the connection table with first connection and
        # traffic times.
        log.debug("Starting traffic...")
        self.connectionTable = {}
        for s in self.servers:
            self._setConnectionTrafficTime(s)

        self.running = True

    @agentmethod()
    def stopTraffic(self, msg):
        """ Implements stopClient from idl """
        self.running = False
        self.nextrun = sys.maxint

    def _setConnectionTrafficTime(self, server):
        now = time.time()
        connectionStart = now + eval(self.connectionInterval)
        trafficStart = connectionStart + eval(self.trafficInterval)
        self.connectionTable[server] = [False, connectionStart, trafficStart,
                                        eval(self.connectionDuration)]
        self._setNextRunTime()

    def _setNextRunTime(self):
        # Find the minimum value for connection time and traffic time
        now = time.time()
        times = []
        for i in [1, 2]:
            t = 0
            if len(self.connectionTable):
                t = min([self.connectionTable[k][i] for k in
                         self.connectionTable.keys()])

            if t < now:
                t = now + 10

            times.append(t)

        self.nextrun = min(times)
        log.info("Next action at : %s (in %s seconds)",
                 (time.ctime(self.nextrun), self.nextrun - time.time()))

    @agentmethod()
    def confirmConfiguration(self):
        """Override the base class implementation to reset the types
        after setting them."""
        # Force types to be correct as they are set to str when
        # set via setConfig()
        # Probably want to wrap this in a try: except.
        self.trafficSize = str(self.trafficSize)
        self.connectionInterval = str(self.connectionInterval)
        self.connectionDuration = str(self.connectionDuration)
        self.trafficInterval = str(self.trafficInterval)
        return True
