
import threading
import asyncore
import logging
import time
import sys
from collections import defaultdict
from magi.util.scheduler import Scheduler
from magi.util import config
from magimessage import MAGIMessage
from transport import Transport
from routerGroup import GroupRouter
from routerNode import NodeRouter
from processor import AckReply, NameAndID, AckRequirement, SequenceRequirement, TimestampRequirement
#from aggregators import TextAggregator, FormattedDataAggregator
from api import GroupRequest, TransportRequest, TransmitRequest, MessageStatus

log = logging.getLogger(__name__)
debug = True


class LocalTransport(Transport):
	def fileno(self):
		return 0
	def __repr__(self):
		return "LocalTransport"
	__str__ = __repr__


class ProcessingQueue(object):

	__slots__ = ['messages', 'processors', 'nextpush', 'flag']
	def __init__(self):
		self.messages = list()
		self.processors = list()
		self.nextpush = -1
		self.flag = False

	def append(self, msg):
		self.messages.append(msg)
		self.flag = True

	def pushRequest(self, when):
		if self.nextpush < when:  # later in time, ignore it
			return
		self.nextpush = when
		self.flag = True


class WorkerThread(threading.Thread):
	"""
		Primary async thread for dealing with multiple sockets, running a scheduler and periodically
		pulling from the user process's transmit queue.  When messages are routed, they are passed by
		reference.

		All requests from the user code will come trough the Queue to keep things thread safe.
	"""
		
	def __init__(self, name, txqueue, rxqueue):
		threading.Thread.__init__(self, name='worker')
		self.setDaemon(True) # This thread won't keep python interpreter running when everything else stops

		# my node name on the network
		self.myname = name

		# keeping track of various stats
		self.stats = defaultdict(int)

		# thread safe queues used to talk to application
		self.txqueue = txqueue
		self.rxqueue = rxqueue

		# transports that we want to keep connected as best we can
		self.requiredList = set()

		# transports that can be sent or received on
		self.transportMap = {0: LocalTransport()}

		# transports that can be passed to poll (have a valid socket fd)
		self.pollMap = dict()

		# scheduler shared by all in this thread including transports
		self.scheduler = Scheduler()

		# create the processors we will use
		ackrep = AckReply()
		nameid = NameAndID()
		groupr = GroupRouter()
		noder = NodeRouter()
		ackreq = AckRequirement()
		seq = SequenceRequirement()
		tstamp = TimestampRequirement()

		# first demo aggregator
		#textagg = TextAggregator()
		#dataagg = FormattedDataAggregator() # how to plugin from above?

		self.routers = [groupr, noder]
		#self.aggregators = [textagg, dataagg]
		#self.processors = [ackrep, nameid, ackreq, seq, tstamp] + self.routers + self.aggregators
		self.processors = [ackrep, nameid, ackreq, seq, tstamp] + self.routers

		for p in self.processors:
			p.configure(name=self.myname, scheduler=self.scheduler, transports=self.transportMap, msgintf=self, stats=self.stats)

#		for p in self.aggregators:
#			p.setIdProcessor(nameid)

		# hook our processors into the various chains
		self.queues = defaultdict(ProcessingQueue)
		self.queues['IN'].processors.extend([ackrep, groupr, noder, ackreq, seq, tstamp])
		self.queues['OUT'].processors.extend([nameid, ackreq, seq, tstamp])
		self.queues['FWD'].processors.extend([groupr])
		#self.queues['PRE'].processors.extend([textagg, dataagg, nameid])
		self.queues['PRE'].processors.extend([nameid])

		self.scheduler.periodic(10, self.printStats)
		

	def printStats(self):
		log.debug("%s", self.stats)


	def processRXSockets(self):
		"""
			Process any data that has showed up in our transports
			Will add new messages to self.PREqueue
		"""
		for fd, transport in self.pollMap.items():
			for msg in transport.inmessages:
				if isinstance(msg, Transport):
					log.info("New transport created: %s", msg)
					self.addTransport(msg)

				elif isinstance(msg, MAGIMessage):
					self.stats['receivedpkts'] += 1
					self.stats['receivedheader'] += msg._orighdrlen
					self.stats['receivedbytes'] += len(msg.data)
					msg._receivedon = transport
					log.debug("RX message: %s", msg)
					log.debug("Appending message to PRE Queue: %s", msg)
					self.queues['PRE'].append(msg)

			transport.inmessages = []



	def processUserRequests(self):
		"""
			Process all the requests that have come through the user API
			Will add transmit messages to OUT
		"""
		while not self.txqueue.empty():
			obj = self.txqueue.get_nowait()
			log.debug("Processing locally created message %s", obj.msg)
			if type(obj.msg.data) not in (str, buffer):
				log.error("Dropping message, data is of type %s, needs to be a string or buffer", type(obj.msg.data))
				return
			obj.msg._receivedon = self.transportMap[0]
			obj.msg._userargs = obj.args
			self.queues['OUT'].append(obj.msg)

#			if isinstance(obj, TransmitRequest):
#				log.debug("Processing locally created message %s", obj.msg)
#				if type(obj.msg.data) not in (str, buffer):
#					log.error("Dropping message, data is of type %s, needs to be a string or buffer", type(obj.msg.data))
#					return
#				obj.msg._receivedon = self.transportMap[0]
#				obj.msg._userargs = obj.args
#				self.queues['OUT'].append(obj.msg)
		
#			elif isinstance(obj, TransportRequest):
#				log.info("Request to add new transport %s to map keepConnected: %s" % (obj.transport, obj.keepConnected))
#				self.addTransport(obj.transport, obj.keepConnected)

#			elif isinstance(obj, GroupRequest):
#				log.debug("Request to %s group %s from local node", obj.type, obj.group)
#				for proc in self.processors:
#					proc.groupRequest(obj)



	def routeMessage(self, msg):
		"""
			Given a message, determine outgoing interfaces to send to
			Will place messages in FWD and/or IN
		"""
		fdlist = set()
		for router in self.routers:
			fdlist.update(router.routeMessage(msg))

		if msg._receivedon is not None:
			if msg._receivedon.fileno() != 0: # We do allow loopback for locally generated messages to make it back
				fdlist.discard(msg._receivedon.fileno())  # not back out same interface, double check
		else:
			log.warning("msg to route but no receiving interface listed: %s", msg)

		log.debug("Routing decides to route %s:%d -> %s/%s out %s", msg.src, msg.msgid, msg.dstnodes, msg.dstgroups, fdlist)
		log.debug("Message %s", msg)
		if not fdlist:
			if msg._receivedon.fileno() == 0:
				log.debug("Dropping locally generated packet with nowhere to go: %s", msg)
				self.stats['nowheretogopackets'] += 1
			else:
				log.debug("Unrouted packet: %s", msg)
				self.stats['unroutedpackets'] += 1
			return

		msg._routed = fdlist
		if 0 in msg._routed:
			log.debug("Appending message to IN queue: %s", msg)
			self.queues['IN'].append(msg)

		if msg._routed == [0]: # don't put it in the FWD queue if there is no reason
			log.debug("No reason to append to forward queue: %s", msg)
			return

		if msg._receivedon.fileno() != 0:
			log.debug("Appending message to forward queue: %s", msg)
			self.queues['FWD'].append(msg)
		else:
			log.debug("Locally generated message place on transport: %s", msg)
			self.placeInTransports(msg) # generated locally, don't pass through FWD processors


	def placeInTransports(self, msg):
		"""
			Take a message that has been routed and place it in the appropriate socket buffers.  Messages destined
			for the local daemon are dealt with separately after IN queue as Queue interface doesn't allow append.
		"""
		for fd in msg._routed:
			if fd == 0:
				log.debug("InTransports: msg for local daemon, move on")
				continue

			if fd in self.transportMap:
				log.debug("Queueing message on %s", self.transportMap[fd])
				msg._appendedto.add(fd)
				self.transportMap[fd].outmessages.append(msg)
				continue

			log.error("Message routed to fd %s, but no such transport", fd)


	def addTransport(self, newTransport, required=False):
		"""
			Add a new transport to our map and give it a scheduler to use
		"""
		log.info("Request to add new transport %s to map keepConnected: %s" % (newTransport, required))
		
		self.pollMap[newTransport.fileno()] = newTransport
		if not newTransport.serverOnly():
			self.transportMap[newTransport.fileno()] = newTransport
		newTransport.setScheduler(self.scheduler)
		
		# 8/6/2013 The transport even though required, where not being 
		# rescheduled for reconnection.     
		# I found that adding the transport to the required list
		# caused the required list to have the same transport more than once
		# I tracked this down the ayncore.py__getattr__() function in py2.7     
		# Now using the str representation returned by __repr__
		# See corresponding change below in function removeTransport 
		if required:
			self.requiredList.add(str(newTransport))

		# Notify processors 
		if not newTransport.serverOnly():
			for proc in self.processors:
				proc.transportAdded(newTransport)


	def removeTransport(self, fd, curTransport):
		"""
			Remove a closed or otherwise dead transport from the active lists, schedule a reconnect
			if the user requested one initially.  We require fd in the arguments as calling fileno()
			on a closed socket will fail.
		"""
		del self.pollMap[fd]
		if fd in self.transportMap:
			del self.transportMap[fd]
			
		# 8/6/2013
		# Now using the str representation returned by __repr__
		# See corresponding change above in function addTransport 
		# If the user wants them always connected and transport can reconnect, attempt later
		if str(curTransport) in self.requiredList:
			log.debug("Transport %s in requiredList", curTransport)
			if getattr(curTransport, 'reconnect', None) is not None:
				log.debug("Transport %s in supports reconnect", curTransport)
			log.info("schedule reconnect for %s in 10 seconds", curTransport)
			#TODO: schedule reconnect after a fixed time period currently 10 seconds 
			# Need to set this a a binary exponential backoff 
			# Typically such requests for reconnection occur when the bridge nodes 
			# are not up and fail 
			self.scheduler.sched_relative(10, self.readdTransport, curTransport)

		# Notify processors
		for proc in self.processors:
			proc.transportRemoved(fd, curTransport)


	def readdTransport(self, oldTransport):
		"""
			Attempt reconnect of an old transport that was removed but user marked as required
		"""
		oldTransport.reconnect()
		self.addTransport(oldTransport)
	
	
	def processGroupRequest(self, obj):
		log.debug("Request to %s group %s from local node", obj.type, obj.group)
		for proc in self.processors:
			proc.groupRequest(obj)

	###### MsgIntf ###

	def send(self, msg):
		""" Let processors 'send' messages by injecting into the start of the out chain """
		msg._receivedon = self.transportMap[0]
		self.queues['OUT'].append(msg)

	def sendDirect(self, msg):
		""" Let subclasses 'send' messages without processing by injecting at the end of the chain """
		msg._receivedon = self.transportMap[0]
		self.placeInTransports(msg)

	def messageStatus(self, txt, isack, msg):
		""" Let subclasses send info back to the main application """
		self.rxqueue.put(MessageStatus(txt, isack, msg))

	def needPush(self, queuename, when):
		self.queues[queuename].pushRequest(when)

	###### MsgIntf ###


	def runChain(self, now, queuename):
		"""
			Run the messages in a queue list through its set processors and return the outcome.
			Will shortcut if there are no messages and nothing in the list needed a push
		"""
		queue = self.queues[queuename]
		queue.nextpush = sys.maxint # reset, will be set again by those in need
		passed = queue.messages 
		queue.messages = []  # reset
		queue.flag = False
		for proc in queue.processors:
			try:
				passed = proc.processMessages(queuename, passed, now)
			except NotImplementedError:
				log.debug("%s doesn't implement process%s", proc, queuename)  # only debug, don't overflow logs with same error

		return passed
	
	
	def processMsgQueues(self):
		"""
			Run all of our processing chains until their input queues are empty
		"""
		now = time.time()
		runs = 10 # safety break, just in case
		while runs > 0 and any([q.flag for q in self.queues.itervalues()]):
			
			# Run PREROUTING processors on received packets, perform routing on those exiting
			if self.queues['PRE'].flag:
				for m in self.runChain(now, 'PRE'):
					log.debug("Routing message from sockets")
					self.routeMessage(m)
	
			# Run INCOMING processors on packets destined for local daemon, send results to user rxqueue
			if self.queues['IN'].flag:
				for m in self.runChain(now, 'IN'):
					log.debug("Queueing message for daemon")
					self.rxqueue.put(m)
	
			# Run OUTGOING processors on packets coming from local machine, most go to router, some have may have routing info already
			if self.queues['OUT'].flag:
				for m in self.runChain(now, "OUT"):
					if m._routed is None:
						log.debug("Routing messages from localhost")
						self.routeMessage(m)
					else:
						log.debug("Direct transport placement from localhost")
						self.placeInTransports(m) 
	
			# Run FORWARD processors on packets that have been routed and are on their way out
			if self.queues['FWD'].flag:
				for m in self.runChain(now, "FWD"):
					log.debug("Placing forwarded packet in transport layer")
					self.placeInTransports(m)

			# Check if we can break from this loop, either we maxed out somehow or all the queues are processed
			runs -= 1

		if runs <= 0:
			log.warning("Ran the queues 10 times, still not empty, can't be right")


	def loop(self):
		"""
			Main processing that takes place on each thread loop iteration.
			- Check transport layer for new messages from network
			- Check user queue for new commands or messages to send
			- Do our queue/chain processing
		"""
		# Check for closed sockets so routing doesn't try and route out them
		for fd, transport in self.pollMap.items():
			if getattr(transport, 'closed', False):
				log.info("Transport %s closed, removing from map", transport)
				lock = threading.Lock()
				lock.acquire() 
				self.removeTransport(fd, transport)
				lock.release()

		# Process receiving sockets
		self.processRXSockets()

		# Process transmit queue from user
		self.processUserRequests()
		
		# Run all of our processing chains until their input queues are empty
		self.processMsgQueues()
		
	def stop(self):
		self.requiredList = set()
		log.debug("Closing transports")
		for transport in self.pollMap.values():
			log.debug("Closing transport %s", transport)
			transport.close()
		log.debug("Transports closed")
		self.done = True

	def run(self):
#		import cProfile
#		cProfile.runctx('self.runX()', globals(), locals(), '/tmp/cprofile_worker')
#	
#	def runX(self):
		"""
			Run the sockets thread.  Infinite loop:
			- run the scheduler and events, returns the time until the next future event
			- wait for that amount of time (max 1/10th second) for events on any open sockets
		"""
		self.threadId = config.getThreadId()
		log.info("Worker started. Thread id: " + str(self.threadId))

		#when = time.time() + 30
		#while time.time() < when:
		self.done = False
		while not self.done:
			try:
				timeout = min(0.2, self.scheduler.run()) # Max 0.2 second sleep, maybe less, we can't wait on Queue.Queue so we have to poll it
				if len(self.pollMap) <= 0:
					time.sleep(timeout)
				else:
					asyncore.poll(timeout, self.pollMap)  # poll appears to be a little faster than poll2
				self.loop()
			except:
				if log is None:
					return  # We shutdown and all my variables are disappearing
				log.error("Failed in router thread: %s", sys.exc_info()[1], exc_info=True)
				time.sleep(0.5) # Don't jump into a super loop on repeatable errors
				
		log.info("Worker stopped")

