

from collections import defaultdict
from magi.messaging.api import MAGIMessage
import heapq
import logging
import random
import sys
import time

log = logging.getLogger(__name__)
debug = False

class MessageProcessor(object):
	"""
		API that a process can handle, creates blank implementations for documentation and processors can decide to implement what they want to.
		Each processor will have access to:
		* nodename, the current node name
		* scheduler, for scheduling activity outside of chain processing
		* msgintf, interface for send, sendDirect and messageStatus when generating new packets and needPush for requesting pushes when no packets traversing
		* transports, the map of ID to transports
		* stats, dictionary of counter values can be added to by processors

		For any chain that it processes, the subclass is expected to implement processX where X is one of (PRE, FWD, IN, OUT)
		processX is simply called with the msglist and returns a msglist for items to passed on to the next processor

		NOTE: a processor has to be added to a chain's list of processors before they will be called
	"""

	def configure(self, name="missing", scheduler=None, msgintf=None, transports=None, stats=None, **kwargs):
		"""  Single conifguration point for all processors """
		self.nodename = name
		self.scheduler = scheduler
		self.transports = transports
		self.msgintf = msgintf
		self.stats = stats


	def processMessages(self, qname, msglist, now):
		""" Allow simpler processors to avoid duplicate code """
		method = 'process'+qname
		if hasattr(self, method):
			return getattr(self, method)(msglist, now)
		raise NotImplementedError("missing %s" % method)


	def scheduleMethod(self, method, when):
		event = self.scheduler.getByMethod(method)
		if event is not None:
			if event.time < when:
				return # earlier event already scheduled, just let that one go instead
			self.scheduler.unsched(event)

		self.scheduler.sched_time(when, method)


	def groupRequest(self, request):
		""" Called when the API calls join or leave are called.  The passed value is a GroupRequest object. Return value is ignored """
		pass

	def transportAdded(self, transport):
		""" Called when a new transport is added to the map. Return value is ignored """
		pass

	def transportRemoved(self, fd, transport):
		""" Called when a transport is removed from the map. Return value is ignored """
		pass


class BlankRouter(MessageProcessor):
	"""
		Extended processor.  Provides an API that a router can use, creates blank implementation
		for documentation and routers can decide to implement what they want to.
	"""
	def routeMessage(self, transport, msg):
		"""
			Called to route the message 'msg' that arrived on 'transport'
			Return a set of the file descriptors (transport keys) that the message should be transported on
		"""
		return set()


class IdList(object):
	"""
		Maintains list of last 149 to 199 unique IDs as a series of 4 sets.
		As a set fills up, the oldest set is tossed and a new one added
	"""

	def __init__(self):
		self.sets = [set()] * 4

	def __contains__(self, val):
		for s in self.sets:
			if val in s:
				return True
		return False

	def __len__(self):
		return sum([len(x) for x in self.sets])

	def add(self, value):
		"""
			Add to our list if not already present, clean as required
		"""
		if value in self:
			return False
		self.sets[-1].add(value)
		if len(self.sets[-1]) >= 50:
			self.sets.pop(0)
			self.sets.append(set())
		return True


class NameAndID(MessageProcessor):
	"""
		Simple processor to update the message src name and message id for outgoing messages
		as well as ensuring dropping of duplicate incoming messages
	"""
	def __init__(self):
		MessageProcessor.__init__(self)
		self.counter = random.randint(1, 2**31)
#		self.counter = 1
		self.lists = defaultdict(IdList)
#		self.lastseenIds = defaultdict(int)

	def processPRE(self, msglist, now):
		""" Check for duplicate messages """
		# TODO: works for only 200 last seen messages
		passed = []
		for msg in msglist:
			log.debug("Checking for duplicate id %s:%d", msg.src, msg.msgid)
			idlist = self.lists[msg.src]
			if idlist.add(msg.msgid):
				passed.append(msg)
			else:
				if debug: log.debug("Dropping duplicate id %s:%d", msg.src, msg.msgid)
				log.debug("Dropping duplicate id %s:%d", msg.src, msg.msgid)
		return passed
	
#	def processPRE(self, msglist, now):
#		""" Check for duplicate messages """
#		""" Assumption: Messages arrive in order """
#		passed = []
#		for msg in msglist:
#			log.info("Checking for duplicate id %s:%d", msg.src, msg.msgid)
#			log.info("Message %s", msg)
#			lastseenid = self.lastseenIds[msg.src]
#			if msg.msgid > lastseenid:
#				passed.append(msg)
#				self.lastseenIds[msg.src] = msg.msgid
#			else:
#				if debug: log.debug("Dropping duplicate id %s:%d", msg.src, msg.msgid)
#				log.info("Dropping duplicate or out-of-order id %s:%d", msg.src, msg.msgid)
#		return passed
	
	def processOUT(self, msglist, now):
		""" Add name and unique id """
		for msg in msglist:
			msg.src = self.nodename
			msg.msgid = self.counter
			log.debug("Added msg id: %s to msg: %s", self.counter, msg)
			self.counter += 1
		return msglist


class AckRequirement(MessageProcessor):
	"""
		Processor sets the ack flag when requested, monitors for the ack and schedules retransmit
	"""
	TIMEOUT = [0.5, 1.0, 2.0, 4.0, 8.0]

	class InflightStore(object):
		__slots__ = ['msg', 'nextsend', 'timerindex']
		def __init__(self, m, n):
			self.msg = m
			self.nextsend = n
			self.timerindex = 0

	def __init__(self):
		MessageProcessor.__init__(self)
		self.inflight = dict()

	def processIN(self, msglist, now):
		""" 
			Pull acks out of the incoming stream and remove the destination from the message.  Once the
			message has no more destination values, we can remove it.
			Ack data looks like:  origmsgid,node,group[,group,...]
		"""
		passed = list()
		for msg in msglist:
			if not msg.isAck():
				passed.append(msg)
				continue
	
			ackdata = msg.data.split(',')
			ackid = int(ackdata[0])
			old = self.inflight.get(ackid, None)
			if old is None:
				if debug: log.debug("Got ack for nothing, duplicate?")
				continue

			# We find out which node or groups this ack is for and remove those from the required list
			if debug: log.debug("Got ack with %s", msg.data)
			old.msg.dstnodes.discard(ackdata[1])
			old.msg.dstgroups.difference_update(ackdata[2:])
			
			if len(old.msg.dstnodes) == 0 and len(old.msg.dstgroups) == 0:  # officially done
				self.inflight.remove(ackid)
				self.msgintf.messageStatus("Ack", True, old.msg)
			else:
				self.checkRetransmits()  # may need to reschedule now
	
		return passed


	def processOUT(self, msglist, now):
		"""
			If the user requested an acknowledgement, set the ACK flag and keep a copy of the message
		"""
		for msg in msglist:
			if msg._userargs.get('acknowledgement', False):
				msg.flags |= MAGIMessage.WANTACK
				self.inflight[msg.msgid] = AckRequirement.InflightStore(msg, now + AckRequirement.TIMEOUT[0])  # store with initial timeout
				self.checkRetransmits()  # may need to reschedule now
			else:
				msg.flags &= ~MAGIMessage.WANTACK

		return msglist


	def checkRetransmits(self):
		"""
			Check for items that need a retransmit, send them and figure how
			long to wait for the next one
			@TODO we may want to store in a heap ordered by nextsend if this list gets large, that way we can cut out early
		"""
		now = time.time()
		earliest = sys.maxint  # determines when we want to next push
		for msgid in self.inflight.keys():
			store = self.inflight[msgid]
			if store.nextsend > now:
				earliest = min(earliest, store.nextsend)
				continue
			
			#TODO: currently tries to retransmit one time less than configured.
			store.timerindex += 1
			if store.timerindex >= len(AckRequirement.TIMEOUT):
				if debug: log.debug("Dropping packet after too many retransmits, ID:%d", msgid)
				del self.inflight[msgid]
				self.msgintf.messageStatus("Dropping packet after too many retransmits", False, store.msg)
			else:
				if debug: log.debug("Retransmit %s", store.msg)
				self.msgintf.sendDirect(store.msg)
				store.nextsend += AckRequirement.TIMEOUT[store.timerindex]
				earliest = min(earliest, store.nextsend)

		self.scheduleMethod(self.checkRetransmits, earliest)


class AckReply(MessageProcessor):
	"""
		Processor that will acknowledge incoming messages that want it 
	"""
	def __init__(self):
		MessageProcessor.__init__(self)
		self.groups = defaultdict(set)
		self.groupset = set()

	def processIN(self, msglist, now):
		""" See if we should ack these messages or not """
		for msg in msglist:
			if msg.wantsAck():
				node = ""
				groups = self.groupset & set(msg.dstgroups)
				if self.nodename in msg.dstnodes:
					# only send node ack if we are a node member, not a group only
					node = self.nodename
					if debug: log.log(7, "NodeAck %s:%d", msg.src, msg.msgid)

				if len(groups) != 0:
					if debug: log.log(7, "GroupAck %s:%d", msg.src, msg.msgid)
				elif node == "":
					if debug: log.debug("No node or group to ack but we are in the INCOMING chain, what?")
					continue  

				ack = msg.createAck(node=node, groups=list(groups))
				ack._receivedon = self.transports[0]
				ack._routed = [msg._receivedon.fileno()] # send ack back out received interface
				self.msgintf.send(ack)  

		return msglist


	def groupRequest(self, req):
		""" Just store info on active group for group acking """
		if req.type == 'join':
			self.groups[req.group].add(req.caller)
		elif req.type == 'leave':
			self.groups[req.group].discard(req.caller)

		# include all keys as long as value set is not empty
		self.groupset = set([g for g, f in self.groups.iteritems() if len(f) > 0])


class SequenceWatcher(object):
	"""
		Maintains sequence order for a single src/dst key
	"""
	def __init__(self, startcounter=-1):
		self.heap = list()
		self.nextid = startcounter

	def add(self, msg):
		heapq.heappush(self.heap, (msg.sequence, msg))

	def getNext(self):
		# Everything sees the queue initially which orders the list by the serial id
		if self.nextid == -1:
			self.nextid = self.heap[0][0]  # read the first serial value and make it the first
	
		# Now draw out everything that follows serial order (could be everything the list or nothing)
		ret = list()
		while len(self.heap) > 0:
			topid = self.heap[0][0]
			if topid < self.nextid:  # Behind the nextid, already processed it, just drop it
				log.info("Dropping old/repeated serial %d", topid)
				heapq.heappop(self.heap)
			elif topid == self.nextid: # The next message we are looking for, pass it on, increment nextid
				(serial, msg) = heapq.heappop(self.heap)
				ret.append(msg)
				self.nextid += 1
			else:
				if debug: log.debug("Hold message %s until previous appears", topid)
				break  # Beyond the next id, we are still waiting for something else

		return ret


class SequenceCounter(object):
	"""
		Keep sequence counter, make sure its used properly, i.e. an ID must have the same dstnodes and dstgroups
	"""
	__slots__=['nhash', 'ghash', 'count']
	def __init__(self, nodes, groups):
		self.nhash = sum(hash(n) for n in nodes) 
		self.ghash = sum(hash(g) for g in groups)
		self.count = 1

	def verifyDest(self, msg):
		nhash = sum(hash(n) for n in msg.dstnodes)
		ghash = sum(hash(g) for g in msg.dstgroups)
		return (self.nhash == nhash) and (self.ghash == ghash)

	
class SequenceRequirement(MessageProcessor):
	"""
		Used for sequence ordering of packets from a particular source.  A separate sequence counter is applied
		to all messages based on the order key provided.  Separate counters are used so that all destinations
		receive the full list of packets in a sequence.  This also means that ordering is only guaranteed for messages that
		are sent to the same destination list otherwise, packets may pile up in the queue awaiting something that will never
		arrive.  For this reason, using the same sequence ID but changing the destination is an error condition.
	"""

	def __init__(self):
		MessageProcessor.__init__(self)
		self.watchers = defaultdict(SequenceWatcher)
		self.counters = defaultdict(SequenceCounter)

	def processIN(self, msglist, now):
		"""
			If the message has as sequence counter, add to the host sequence watcher and then extract
			whatever messages are available
		"""
		passed = list()
		for msg in msglist:
			if msg.sequence is None or msg.sequenceid is None:  # nothing will have changed for us
				passed.append(msg)
				continue
			
			watcher = self.watchers[(msg.src,msg.sequenceid)]
			watcher.add(msg)
			passed.extend(watcher.getNext()) # pull out and return list

		return passed


	def processOUT(self, msglist, now):
		"""
			If the user specified a sequenceID, then get the active counter for that ID and apply to message.
			Also verify that any messages using the same sequence ID also use the same destination lists.  If
			not, return an error to the user
		"""
		passed = list()
		for msg in msglist:
			if 'source_ordering' in msg._userargs:
				seqid = int(msg._userargs['source_ordering'])
				if seqid not in self.counters:
					# new sequence id
					self.counters[seqid] = SequenceCounter(msg.dstnodes, msg.dstgroups)
				else:
					# used id, make sure dstnodes and dstgroups are the same, set comparisons
					current = self.counters[seqid]
					if not current.verifyDest(msg):
						self.msgintf.messageStatus("Destination parameters cannot change in a sequence", False, msg)
						continue
					current.count += 1

				msg.sequenceid = seqid
				msg.sequence = self.counters[seqid].count
			else:
				msg.sequence = None

			passed.append(msg)

		return passed


class TimestampRequirement(MessageProcessor):

	def __init__(self):
		"""
			Just creates a heap structure for timestamp checking
		"""
		MessageProcessor.__init__(self)
		self.heap = list()

	def processIN(self, msglist, now):
		"""
			If the message has a timestamp requirement add to the heap and pull out anything that is past our current time.
			If no timestamp, it automatically passes
		"""
		passed = list()
		for msg in msglist:
			if msg.timestamp is None:  # this one always goes
				passed.append(msg)
				continue
			heapq.heappush(self.heap, (msg.timestamp, msg))  # Stick commands on the heap indexed by time

		# Pull out any passed current time
		# TODO: deal with timezones and offsets, etc
		nexttime = sys.maxint
		while len(self.heap) > 0:
			nexttime = self.heap[0][0]
			if nexttime > now:
				if debug: log.debug("TimetampRequirement waits for %f [%d]", (nexttime - now), len(self.heap))
				self.msgintf.needPush('IN', nexttime)
				break
			passed.append(heapq.heappop(self.heap)[1])
		
		return passed


	def processOUT(self, msglist, now):
		""" If the user requested timestamp delivery, add it to the message """
		for msg in msglist:
			if 'timestamp' in msg._userargs:
				msg.timestamp = int(msg._userargs['timestamp'])
			else:
				msg.timestamp = None
		return msglist

