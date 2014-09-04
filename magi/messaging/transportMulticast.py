
from transport import Transport
from magimessage import DefaultCodec
from magi.util.Collection import namedtuple
import socket
import struct
import logging
import random
import time

log = logging.getLogger(__name__)
RXRequest = namedtuple("RXRequest", "start, end, time")
debug = True


class RXMessageTracker(object):
	""" Tracks a single message from a single neighbor node """

	__slots__ = ['data', 'missing', 'complete', 'scheduled', 'src', 'msgid', 'sched', 'txqueue']

	def __init__(self, src, msgid, sched, txqueue):
		self.data = None
		self.missing = {0:0}  # Map of part ID to last request time
		self.complete = False
		self.scheduled = None
		self.src = src
		self.msgid = msgid
		self.sched = sched
		self.txqueue = txqueue

	def msg_addPacket(self, part, total, piece):
		""" Record a new piece of data """
		if self.complete:
			# already done, extra packet, ignore it
			return 

		if self.data is None:
			# first time in, initialize our state
			self.data = [None]*total
			del self.missing[0]
			self.missing = dict([(k, 0) for k in range(1,total+1)])  # dict with part numbers as keys, 0 for each value

		# apply the piece
		self.data[part-1] = piece

		# GTL this del() is raising a KeyError very occasionally on large
		# container experiments (1000+ nodes). So check before deleting
		# and figuring out where this error is coming from.
		if part in self.missing:
			del self.missing[part]

		if not self.missing:
			self.complete = True
			self.msg_unscheduleAll()
		else:
			self.msg_schedRequest()


	def msg_getData(self):
		"""
			Get the packet data as a tuple of header and data portions
			Assumes at least length number and header will fit into the first packet
		"""
		(totallen, hdrlen) = struct.unpack('>IH', self.data[0][:6])
		realhlen = hdrlen + 6  # read in header plus the two length values

		header = self.data.pop(0)
		while len(header) < realhlen:
			header += self.data.pop(0)

		if len(header) > realhlen:
			self.data.insert(0, header[realhlen:])
			header = header[:realhlen]
		
		return (header, ''.join(self.data))


	def msg_requestMade(self, sched, pieces):
		""" Indicates that a request was made for this message """
		now = time.time()
		if self.data is None:
			if pieces[0] == 0:
				self.missing[0] = now
		else:
			for p in pieces:
				if p in self.missing:
					self.missing[p] = now

		self.msg_schedRequest()


	def msg_schedRequest(self):
		"""
			Determine what to request, unschedule unncessary and schedule requirements
		"""
		if self.complete:  # quick escape
			return

		if self.scheduled is not None: # already have something scheduled, ignore
			return 

		threshold = time.time() - 5  # more than 5 seconds ago
		if self.data is None:
			if self.missing[0] > threshold:
				return
		else:
			if not any([k for k,t in self.missing.iteritems() if t < threshold]):
				return
			
		self.scheduled = self.sched.sched_relative(random.uniform(0.5, 3), self.makeRequest)


	def msg_unscheduleAll(self):
		if self.scheduled is not None:
			self.sched.unsched(self.scheduled)
			self.scheduled = None

	def makeRequest(self):
		self.scheduled = None

		if self.complete:
			if debug: log.debug("scheduledRequest called but we are already done, moving along")
			return

		now = time.time()
		threshold = now - 5  # more than 5 seconds ago
		if self.data is None:  # Request full message
			if self.missing[0] > threshold:
				return  # skip it, someone beat us to the punch
			self.missing[0] = now
			request = MCTHeader.PktReq(self.msgid, self.src, 0)  # All pieces
		else:
			ids = [k for k,t in self.missing.iteritems() if t < threshold]
			if not ids:
				return # again, someone beat us somehow, ignore and move on
			for k in ids:
				self.missing[k] = now
			request = MCTHeader.PktReq(self.msgid, self.src, *ids)

		self.txqueue.insert(0, request) # insert request at head of queue


class MessageCompleteError(Exception):
	pass


class NeighborTracker(object):
	"""
		Class to track detail of a single neighbor in our multicast transport domain.
		It tracks the sender's info as well as all of the messages from the sender.
		If it determines that we need to make a request, it will schedule one.
	"""

	__slots__ = ['addr', 'sched', 'queue', 'codec', 'boottime', 'lastid', 'lastlinear', 'completeset', 'messages']

	def __init__(self, addr=None, sched=None, queue=None, codec=None):
		self.addr = addr
		self.sched = sched
		self.queue = queue
		self.codec = codec
		self.boottime = 0 # the boot time from the last status message to detect restarts
		self.lastid = 0   # last id as reported by neighbor
		self.lastlinear = 0  #  the last complete id in the linear integer line
		self.completeset = set()  # complete ids but still missing in between so we can't update lastlinear
		self.messages = dict()  # the list of messages being built


	def neigh_adjustWaiting(self):
		""" 
			Readjust are message tracking values when other state changes
		"""
		try:
			while True:
				self.completeset.remove(self.lastlinear+1)
				self.lastlinear += 1
		except KeyError:
			pass
		if debug: log.debug("adjusted waiting, now %d %s", self.lastlinear, self.completeset)


	def neigh_getMessageTracker(self, msgid):
		"""
			Get the tracker for this message.  If its already compelted a MessageCompleteError
			will be thrown to indicate that the message is no longer being processed.
		"""
		if msgid <= self.lastlinear or msgid in self.completeset:
			raise MessageCompleteError("This message is complete")  # Already completed
		if msgid not in self.messages:
			self.messages[msgid] = RXMessageTracker(self.addr, msgid, self.sched, self.queue)
		return self.messages[msgid]


	def neigh_dataPacket(self, msgid, part, total, data):
		"""
			Received packet data from this neighbor, find out where it goes and adjust 
			tracking as needed.
		"""
		try:
			msgtracker = self.neigh_getMessageTracker(msgid)
			msgtracker.msg_addPacket(part, total, data)
			
			if msgtracker.complete:
				hdrbuf, data = msgtracker.msg_getData()
				msg, hdrsize = self.codec.decode(hdrbuf)
				msg.data = data
				del self.messages[msgid]
				self.completeset.add(msgid)
				self.neigh_adjustWaiting()
				return msg
		except MessageCompleteError:
			if debug: log.debug("Ignoring data for %s/%d, already completed", self.addr, msgid)

		return None


	def neigh_currentId(self, msgid, boottime):
		"""
			Record the latest sent id according to the neighbor
		"""
		log.debug("CurrentID %s from %s", msgid, self.addr)
		if boottime != self.boottime:
			log.debug("New boottime (%s) for %s, reseting tracking info to start with %d", boottime, self.addr, msgid)
			self.boottime = boottime
			self.lastlinear = msgid  # don't do the restransmit 1 previous (msgid-1) or we can get really old messages that cause stale issues
			self.completeset = set()
			self.messages = dict()

		self.lastid = msgid
		if self.lastlinear < self.lastid:
			if debug: log.debug("Src indicates finished %d, we've only completed up to %d", self.lastid, self.lastlinear)
			for index in range(self.lastlinear+1, self.lastid+1):
				if index in self.completeset: continue
				try:
					self.neigh_getMessageTracker(index).msg_schedRequest()
				except MessageCompleteError:
					pass


	def neigh_packetDead(self, msgid):
		"""
			Record that this packet is dead.  Remove from requirements
		"""
		if msgid < self.lastlinear:
			return # nothing needs to be done, we're already past this
		if msgid in self.messages:
			mtracker = self.messages.pop(msgid) # remove from tracking
			mtracker.msg_unscheduleAll()
		self.completeset.add(msgid)
		self.neigh_adjustWaiting()


	def neigh_requestMade(self, msgid, pieces):
		""" I or someone else made a request for retransmit, adjust timers """
		try:
			self.neigh_getMessageTracker(msgid).msg_requestMade(self.sched, pieces)
		except MessageCompleteError:
			return  # we don't care


		
class TXTracker(object):
	"""
		The None of tracker classes
	"""
	def __init__(self): pass
	def getId(self): return None
	def isDone(self): return True


class TXMessageTracker(TXTracker):
	"""
		This class tracks any message that a multicast transport is sending.  It provides a segmented
		view of the messages as a large array of bytes, letting the caller call for each section in turn.  Each
		returned section is also prepended with the appropriate layer header to indentify the part, total and message
		ID.  SPLITSIZE could potentially differ between multicast transports and different transports would be on
		different counters so each transport will create their own tracker.
	"""

	SPLITSIZE = 1450

	def __init__(self, msg=None, multicastid=None, codec=None):
		""" Create a new parts tracker that tracks this particular message """
		self.msg = msg
		if self.msg.data is None:
			self.msg.data = ""
		self.multicastid = multicastid
		self.eheader = codec.encode(self.msg)
		self.start2 = self.SPLITSIZE - len(self.eheader)
		self.parts = (len(self.eheader) + len(self.msg.data) - 1)/self.SPLITSIZE + 1
		self.queueAll()

	def getId(self):
		return self.multicastid

	def queue(self, pieces):
		if len(pieces) > 0 and pieces[0] == 0:
			self.queueAll()
		else:
			self.tosend.update(pieces)

	def queueAll(self):
		self.tosend = set(xrange(1, self.parts+1))
		
	def isDone(self):
		return len(self.tosend) == 0

	def getNext(self):
		""" Get the next piece to send based on how many previous calls we've had """
		if self.isDone():
			return None
		p = self.getPart(self.tosend.pop())
		return p

	def getPart(self, partnum):
		""" Return a single partition of the serial version of this message along with its MCT header """
		if partnum < 1 or partnum > self.parts:
			raise IndexError("Invalid part number")

		info = MCTHeader.PktData(self.multicastid, partnum, self.parts).encode()
		if partnum == 1:  # Message header in addition to data
			return info + self.eheader + self.msg.data[0:self.start2]

		start = self.start2 + ((partnum-2) * self.SPLITSIZE)
		return info + self.msg.data[start : start+self.SPLITSIZE]



class TXControlTracker(TXTracker):
	"""
		Implements the same interface as TXMessageTracker but is setup to work with plan MCTHeader objects
		used for non data messages.
	"""
	def __init__(self, msg):
		""" Create a new parts tracker that tracks this particular message """
		self.data = msg.encode()
		self.sent = False

	def getId(self):
		return None

	def queue(self, pieces):
		self.sent = False

	def isDone(self):
		return self.sent

	def getNext(self):
		""" Get the next piece to send based on how many previous calls we've had """
		if self.sent:
			return None
		self.sent = True
		return self.data



class MCTHeader(object):
	"""
		This is the header that is prepended to any data sent in a UDP packet from this transport.
		Some packets may only contain this header while the data packets also contain some portion of
		a MAGIMessage
	"""
	PKTDATA = 0 # packet of real  data
	PKTREQ  = 1 # request to retransmit data
	PKTSTAT = 2 # current status of this nodes transmitted
	PKTDEAD = 3 # response to request for retransmit of an ID we don't have

	strs = {PKTDATA:"PktData", PKTREQ:"PktReq", PKTSTAT:"PktStat", PKTDEAD:"PktDead"}

	def __init__(self, type, *args):
		self.type = type
		self.multicastid = args[0]
		
		if type == MCTHeader.PKTDATA:
			self.partnum = args[1]
			self.partcount = args[2]

		elif type == MCTHeader.PKTREQ:
			if len(args[1]) == 4:
				self.src = socket.inet_ntoa(args[1])
			else:
				self.src = args[1]
			self.pieces = args[2:]

		elif type == MCTHeader.PKTSTAT:
			self.boottime = args[1]


	def __repr__(self):
		if self.type == MCTHeader.PKTDATA:
			return "PktData %s, %s of %s" % (self.multicastid, self.partnum, self.partcount)

		elif self.type == MCTHeader.PKTREQ:
			return "PktReq %s to %s for %s" % (self.multicastid, self.src, self.pieces)

		elif self.type == MCTHeader.PKTSTAT:
			return "PktStat %s, boottime %s" % (self.multicastid, self.boottime)

		elif self.type == MCTHeader.PKTDEAD:
			return "PktDead %s" % (self.multicastid)
		return "Invalid MCT"

		

	def encode(self):
		if self.type == MCTHeader.PKTDATA:
			return struct.pack('>BHHH', self.type, self.multicastid, self.partnum, self.partcount)

		elif self.type == MCTHeader.PKTREQ:
			return struct.pack('>BH4s%dH' % len(self.pieces), self.type, self.multicastid, socket.inet_aton(self.src), *self.pieces)

		elif self.type == MCTHeader.PKTSTAT:
			return struct.pack('>BHL', self.type, self.multicastid, self.boottime)

		elif self.type == MCTHeader.PKTDEAD:
			return struct.pack('>BH', self.type, self.multicastid)

		raise IOError("Can't encode type %d" % self.type)


	@classmethod
	def PktData(cls, mid, partnum, partcount):
		return MCTHeader(MCTHeader.PKTDATA, mid, partnum, partcount)

	@classmethod
	def PktReq(cls, mid, src, *pieces):
		return MCTHeader(MCTHeader.PKTREQ, mid, src, *pieces)

	@classmethod
	def PktStat(cls, mid, boottime):
		return MCTHeader(MCTHeader.PKTSTAT, mid, boottime)

	@classmethod
	def PktDead(cls, mid):
		return MCTHeader(MCTHeader.PKTDEAD, mid)

	@classmethod
	def decode(cls, data):
		"""
			Reads packet header
			PKTDATA - a piece of a MAGIMessage with the header (multicastmsgid, part#, totalparts)
			PKTREQ - a request to retransmit a range of pieces of a particular messages (multicastmsgid, part#begin, part#end)
			PKTSTAT - periodic indicator of the last message id that this node transmitted
			PKTDEAD - response to PKTREQ when the sender has no record of the packet (never sent or no longer available)
		"""
		ptype = ord(data[0])
		if ptype == MCTHeader.PKTDATA:
			return MCTHeader(*struct.unpack('>BHHH', data[:7]))

		elif ptype == MCTHeader.PKTREQ:
			piececount = (len(data) - 7)/2
			return MCTHeader(*struct.unpack('>BH4s%dH' % piececount, data))

		elif ptype == MCTHeader.PKTSTAT:
			return MCTHeader(*struct.unpack('>BHL', data[:7]))

		elif ptype == MCTHeader.PKTDEAD:
			return MCTHeader(*struct.unpack('>BH', data[:3]))

		return None




class MulticastTransport(Transport):
	""" 
		Group multicast communication.
	"""
	def __init__(self, address=None, port=None, localaddr=None, droppercent=0.0, codec=DefaultCodec):
		"""
			Create a new multicast transport.
			 addr
				the multicast address to join
			 port
				the port to listen on
			 localaddr
				the address of the local interface to stick to for receiving
			droppercent
				for testing only, transmitter will drop x% (0-1.0) of packets at network layer
		"""

		Transport.__init__(self, codec=codec)
		self.addr = address
		self.port = port
		self.localaddr = localaddr
		self.droppercent = droppercent
		self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)

		self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		if (hasattr(socket, "SO_REUSEPORT")):
			self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
		self.socket.bind((address, port))
		self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(address)+socket.inet_aton(localaddr))
		# Set the multicast interface in case we are using external tunnels or the machine
		# for some reason has a default route we do not like.
		# Alefiya: does this resolve the large number of drops? 
		#self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(localaddr))

		self.finished = dict()
		self.neighbors = dict()
		self.idcounter = 0
		self.boottime = int(time.time())
		self.txMessage = TXTracker() # blank tracker, forces dequeing of next


	def __repr__(self):
		return "MulticastTransport %s:%d" % (self.addr,self.port)
	__str__ = __repr__


	def handle_error(self):
		log.error("Error on %s", self, exc_info=True)


	def setScheduler(self, sched):
		""" Intercept setting of scheduler so we know when to start our periodic timer """
		Transport.setScheduler(self, sched)
		# Alefiya: added sendStatus every 10 second in again 
 		# the first status message is jittered across all the nodes  
		# Not sure if the jitter is a good idea for aggregation purposes 
		#jitter = random.randint(0,20)
		#self.sched.sched_relative(30+jitter,self.sendStatus) 
		self.sendStatus()
		
	def sendStatus(self):
		""" Queue a status message to send """
		# GTL - note: this is not currently called so status is not sent.
		self.outmessages.insert(0, MCTHeader.PktStat(self.idcounter, self.boottime))
		self.sched.sched_relative(10, self.sendStatus)

	def handle_read(self):
		""" Reads a packet of data and processes.  """
		(data, srcaddr) = self.socket.recvfrom(65536)
		if data is None or srcaddr is None or srcaddr[0] == self.localaddr:
			return

		if debug: log.debug("Read %d bytes from %s", len(data), srcaddr)
		self.processPacket(srcaddr[0], srcaddr[1], data)

	def processPacket(self, srchost, srcport, data):
		header = MCTHeader.decode(data)
		if header is None:
			log.error("Invalid header data received len: %d start: '%s'", len(data), data[0:10])
			return

		if srchost not in self.neighbors:
			self.neighbors[srchost] = NeighborTracker(addr=srchost, sched=self.sched, queue=self.outmessages, codec=self.codec)
		ntracker = self.neighbors[srchost]

		if header.type == MCTHeader.PKTDATA:
			if debug: log.log(7, "Received data packet for %d - %d of %d", header.multicastid, header.partnum, header.partcount)
			msg = ntracker.neigh_dataPacket(header.multicastid, header.partnum, header.partcount, data[7:])
			if msg is not None: # We completed a messages with this packet
				self.inmessages.append(msg)

		elif header.type == MCTHeader.PKTSTAT:
			if debug: log.log(7, "Received stat packet, %s reports lastid %d, checking to see if we are missing anything", srchost, header.multicastid)
			ntracker.neigh_currentId(header.multicastid, header.boottime)

		elif header.type == MCTHeader.PKTDEAD:
			if debug: log.debug("Received dead packet indicator for %d, removing from our list of requests", header.multicastid)
			ntracker.neigh_packetDead(header.multicastid)

		elif header.type == MCTHeader.PKTREQ:
			# Request packet, if we are the source, retransmit, if not, apply NACK supression as required
			if debug: log.debug("Received request for packet %s/%d", header.src, header.multicastid)
			if header.src == self.localaddr:
				if header.multicastid in self.finished:
					if debug: log.debug("I am the source of the packet, requeing message with %s", header.pieces)
					msg = self.finished.pop(header.multicastid)
					msg.queue(header.pieces)
					self.outmessages.insert(0, msg)   # TODO: Need to check if already in outmessages
				elif self.txMessage.getId() == header.multicastid:
					if debug: log.debug("Adding parts back to current transmission %s", header.pieces)
					self.txMessage.queue(header.pieces)
				else:
					if debug: log.debug("Message not available, indicating dead")
					self.outmessages.insert(0, MCTHeader.PktDead(header.multicastid))
			else:
				if debug: log.log(7, "Request for someone else, making note in case we are missing it too (squelch NACK's)")
				if header.src not in self.neighbors:
					self.neighbors[header.src] = NeighborTracker(addr=header.src, sched=self.sched, queue=self.outmessages, codec=self.codec)
				self.neighbors[header.src].neigh_requestMade(header.multicastid, header.pieces)


	def readable(self):
		return True


	def getNextTracker(self):
		msg = self.outmessages.pop(0)
		if isinstance(msg, TXTracker):
			return msg
		elif isinstance(msg, MCTHeader):
			return TXControlTracker(msg)
		else:
			self.idcounter += 1
			return TXMessageTracker(msg=msg, multicastid=self.idcounter, codec=self.codec)
		

	def handle_write(self):
		""" Send one UDP packet, let poll determine writablility and spread processing over other sockets """
		if self.txMessage.isDone():
			# Done with this message, move onto used queue, file away based on the multicast transport id
			if self.txMessage.getId() is not None:
				self.finished[self.txMessage.getId()] = self.txMessage
			self.txMessage = self.getNextTracker()
			return

		data = self.txMessage.getNext()
		if random.random() < self.droppercent:
			if debug: log.debug("dropping %d bytes for %s", len(data), self.addr)
			return
		if debug: log.debug("sending %d bytes to %s", len(data), self.addr)
		self.socket.sendto(data, (self.addr, self.port))


	def writable(self):
		return not self.txMessage.isDone() or len(self.outmessages) > 0



