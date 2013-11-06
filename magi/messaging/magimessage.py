
import struct
import logging

log = logging.getLogger(__name__)
debug = False

class MAGIMessage(object):
	""" 
		Represents the message object for sending messages between MAGI daemons.
	"""

	NONE = 0
	BLOB = 1
	TEXT = 2
	IMAGE = 3
	PROTOBUF = 4
	YAML = 5
	XML = 6

	ISACK = 1
	NOAGG = 2
	WANTACK = 4

	OPTIONS = {
		1: 'sequence',
		2: 'timestamp',
		3: 'sequenceid',
		4: 'hosttime',
		20: 'src',
		21: 'srcdock',
		22: 'hmac',
		50: 'dstnodes',
		51: 'dstgroups',
		52: 'dstdocks'
	}

	def __init__(self, **kwargs):
		""" 
			Create a default message. 
			kwargs can be used to specify message components
			contenttype, data, groups, nodes, docks, sequence, timestamp, src, srcdock, hmac
		"""
		self.msgid = 0
		self.flags = 0
		self.contenttype = kwargs.pop('contenttype', MAGIMessage.NONE)
		self.data = kwargs.pop('data', None)

		self.dstgroups = kwargs.pop('groups', set())
		self.dstnodes  = kwargs.pop('nodes', set())
		self.dstdocks  = kwargs.pop('docks', set())

		if type(self.dstgroups) is list:
			self.dstgroups = set(self.dstgroups)
		elif type(self.dstgroups) is str:	
			self.dstgroups = set([s.strip() for s in self.dstgroups.split(',')])
		elif self.dstgroups is None:
			self.dstgroups = set()

		if type(self.dstnodes) is list:
			self.dstnodes = set(self.dstnodes)
		elif type(self.dstnodes) is str:
			self.dstnodes = set([s.strip() for s in self.dstnodes.split(',')])
		elif self.dstnodes is None:
			self.dstnodes = set()

		if type(self.dstdocks) is list:
			self.dstdocks = set(self.dstdocks)
		elif type(self.dstdocks) is str:
			self.dstdocks = set([s.strip() for s in self.dstdocks.split(',')])
		elif self.dstdocks is None:
			self.dstdocks = set()

		self.sequence  = kwargs.pop('sequence', None)
		self.sequenceid  = kwargs.pop('sequenceid', None)
		self.timestamp = kwargs.pop('timestamp', None)
		self.hosttime = kwargs.pop('hosttime', None)

		self.src       = kwargs.pop('src', None)
		self.srcdock   = kwargs.pop('srcdock', None)
		self.hmac      = kwargs.pop('hmac', None)

		# Internals, not used on the wire
		self._receivedon = None   # interface we were received on
		self._appendedto = set()  # marks when message is appended to an outgoing queue
		self._routed = None		  # marks when a message is routed to particular transports
		self._userargs = {}		  # for messages entering locally, the user delivery args
		self._orighdrlen = 0      # for statistics

		if len(kwargs) > 0:
			log.error("Unknown arguments for MAGIMessage (%s)", kwargs)


	def isAck(self):
		return (self.flags & MAGIMessage.ISACK) != 0

	def wantsAck(self):
		return (self.flags & MAGIMessage.WANTACK) != 0

	def dontAggregate(self):
		return (self.flags & MAGIMessage.NOAGG) != 0

	def createAck(self, node="", groups=[]):
		ack = MAGIMessage()
		if self.src is None:
			raise IOError("Message has no src, can't create an acknowledgement")
		ack.dstnodes = set([self.src])
		if self.srcdock is not None:
			ack.dstdocks = set([self.srcdock])
		ack.flags = MAGIMessage.ISACK
		ack.data = ','.join([str(self.msgid), node] + groups)
		return ack


	def __repr__(self):
		if self.data is None:
			data = "None"
		else:
			# data = self.data[0:10]
			data = self.data

		return "In:%s,Out:%s (msgid:%s,flags:0x%X,conttype:%s) src:dock - %s:%s --> dstgroups:%s, dstnodes: %s, dstdocks: %s = data: %s"  % (self._receivedon, self._routed, self.msgid, self.flags, self.contenttype, self.src, self.srcdock, self.dstgroups, self.dstnodes, self.dstdocks, data)


class DefaultCodec(object):
	"""
		A separate codec for encoding and decoding MAGI messages on via a messaging system transport.
		Note all codec outputs must start with 6 bytes.  4 bytes for total message length and 2 bytes
		for header length.  See :doc:`../messaging/wire` for more info.
	"""

	@classmethod
	def encode(cls, msg):
		"""
			Return an encoded wire format version of the header for this message as it stands.
			TotalLength - 4 bytes
			HeaderLength - 2 bytes
			Encoded header pieces
		"""
		options = list()
		for key, name in MAGIMessage.OPTIONS.iteritems():
			val = getattr(msg, name)
			if val is not None:
				if key < 20:
					options.append(struct.pack('>BBI', key, 4, val))
				elif key < 50:
					options.append(struct.pack('>BB%ds' % len(val), key, len(val), val))
				else:
					for item in val:
						if item is None:
							log.warning("Got a None value in key %s, skipping", key)
						else:
							options.append(struct.pack('>BB%ds' % len(item), key, len(item), item))

		optionstr = ''.join(options)
		headerlen = 6 + len(optionstr)
		totallen =  2 + headerlen
		if msg.data is not None:
			totallen += len(msg.data)
		return struct.pack('>IHIBB', totallen, headerlen, msg.msgid, msg.flags, msg.contenttype) + optionstr
		

	@classmethod
	def decode(cls, headerbuf):
		"""
			Decodes header data and returns a new MAGIMessage with header information in a tuple with the total header size
			headerbuf should include the same data as returned from encode
		"""
		newmsg = MAGIMessage()
		(totallen, hdrlen, newmsg.msgid, newmsg.flags, newmsg.contenttype) = struct.unpack('>IHIBB', headerbuf[0:12])
		newmsg._orighdrlen = hdrlen

		idx = 12
		while idx < hdrlen+6:
			(htype, hlen) = struct.unpack('>BB', headerbuf[idx:idx+2])
			idx += 2
			hname = MAGIMessage.OPTIONS.get(htype, None)
			if debug: log.log(5, "setting option %s", hname)

			if hname is None: 
				log.warning("Don't understand header option %d, skipping", htype)
			elif htype < 20:
				setattr(newmsg, hname, struct.unpack('>I', headerbuf[idx:idx+hlen])[0])
			elif htype < 50:
				setattr(newmsg, hname, headerbuf[idx:idx+hlen])
			else:
				getattr(newmsg, hname).add(headerbuf[idx:idx+hlen])

			idx += hlen

		return newmsg, hdrlen+6

