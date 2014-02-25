from magi.messaging.api import DefaultCodec, MAGIMessage as APIMagiMessage
from magi.messaging.transportStream import RXTracker, TXTracker
import fcntl
import logging
import os
import struct
import yaml

log = logging.getLogger(__name__)

class AgentRequest(object):
	"""
		Object form of agent request across the process boundaries
	"""
	JOIN_GROUP = 1
	LEAVE_GROUP = 2
	LISTEN_DOCK = 3
	UNLISTEN_DOCK = 4
	MESSAGE = 5

	OPTIONS = {
		1: 'acknowledgement',
		2: 'source_ordering',
		3: 'timestamp',
	}

	def __init__(self, **kwargs):
		self.request = kwargs.pop('request', None)
		self.data    = kwargs.pop('data', None)

		for key, name in AgentRequest.OPTIONS.iteritems():
			if name in kwargs:
				setattr(self, name, kwargs[name])

		if len(kwargs) > 0:
			log.error("Unknown arguments for AgentRequest (%s)", kwargs)

	@classmethod
	def JoinGroup(cls, grp):
		return AgentRequest(request=AgentRequest.JOIN_GROUP, data=grp)

	@classmethod
	def LeaveGroup(cls, grp):
		return AgentRequest(request=AgentRequest.LEAVE_GROUP, data=grp)

	@classmethod
	def ListenDock(cls, dock):
		return AgentRequest(request=AgentRequest.LISTEN_DOCK, data=dock)

	@classmethod
	def UnlistenDock(cls, dock):
		return AgentRequest(request=AgentRequest.UNLISTEN_DOCK, data=dock)

	@classmethod
	def MAGIMessage(cls, msg, **options):
		hdr = DefaultCodec().encode(msg)
		data = msg.data
		if data is None:
			data = ""
		return AgentRequest(request=AgentRequest.MESSAGE, data=hdr+data, **options)


class AgentCodec(object):
	"""
		Codec for encoding and decoding messages on the agent interface.
	"""

	def encode(self, msg):
		"""
			Return an encoded wire format version of the header for this message as it stands.
			TotalLength - 4 bytes
			HeaderLength - 2 bytes
			Encoded header pieces
		"""
		options = list()
		for key, name in AgentRequest.OPTIONS.iteritems():
			val = getattr(msg, name, None)
			if val is not None:
				if key == 3:
					options.append(struct.pack('>BBI', key, 4, val))
				else:
					options.append(struct.pack('>BB', key, 0))

		optionstr = ''.join(options)
		headerlen = 1 + len(optionstr)
		totallen =  2 + headerlen
		if msg.data is not None:
			totallen += len(msg.data)
		return struct.pack('>IHB', totallen, headerlen, msg.request) + optionstr
		


	def decode(self, headerbuf):
		"""
			Decodes header data and returns a new AgentRequest with header information minus data
			headerbuf should include the same data as returned from encode
		"""
		newmsg = AgentRequest()
		(totallen, hdrlen, newmsg.request) = struct.unpack('>IHB', headerbuf[0:7])

		idx = 7
		while idx < hdrlen+6:
			(htype, hlen) = struct.unpack('>BB', headerbuf[idx:idx+2])
			idx += 2
			hname = AgentRequest.OPTIONS.get(htype, None)
			log.log(5, "setting option %s", hname)

			if hname is None: 
				log.warning("Don't understand header option %d, skipping", htype)
			elif htype == 3:
				setattr(newmsg, hname, struct.unpack('>I', headerbuf[idx:idx+hlen])[0])
			else:
				setattr(newmsg, hname, True)
			idx += hlen

		return newmsg, hdrlen+6


class MessageReader(object):
	""" Used for processing incoming agent messages from a file descriptor """
	def __init__(self, fd, blocking):
		self.fd = fd
		self.blocking = blocking
		self.codec = AgentCodec()
		self.msgCodec = DefaultCodec()
		self.incoming = RXTracker(codec=self.codec)

	def poll(self):
		buf = os.read(self.fd, 4096)
		if self.blocking and buf == "":
			raise IOError("EOF on stream, file descriptor closed")
		self.incoming.processData(buf)
		if self.incoming.isDone():
			msg = self.incoming.getMessage()
			log.debug("message in is %s, %s", AgentRequest.OPTIONS.get(msg.request, msg.request), msg)
			self.incoming = RXTracker(startbuf=self.incoming.getLeftover(), codec=self.codec)
			if msg.request == AgentRequest.MESSAGE:  # If its a MAGIMessage, decode now
				magimsg, hdrsize = self.msgCodec.decode(msg.data)
				log.debug("decode magi message portion (%s, %s)", hdrsize, magimsg)
				magimsg.data = msg.data[hdrsize:]
				msg.data = magimsg
			return msg
		return None


class MessageSender(object):
	""" Used for sending agent messages out a file descriptor """

	def __init__(self, fd, request):
		self.fd = fd
		self.outgoing = TXTracker(codec=AgentCodec(), msg=request)

	def poll(self):
		if self.outgoing.isDone():
			log.log(1, "already done")
			return True
		data = self.outgoing.getData()
		log.log(1, "attempt send %d", len(data))
		self.outgoing.sent(os.write(self.fd, self.outgoing.getData()))
		log.log(1, "return %s", self.outgoing.isDone())
		return self.outgoing.isDone()


class AgentInterface(object):
	""" The python interface for an external process agent.  Used by the daemon and python based process agents """

	def __init__(self, infd, outfd, blocking = True):
		self.infd = infd
		self.outfd = outfd
		self.blocking = blocking
		if blocking:
			fcntl.fcntl(infd, fcntl.F_SETFL, ~os.O_NONBLOCK & fcntl.fcntl(infd, fcntl.F_GETFL))
		self.incoming = MessageReader(self.infd, self.blocking)

	def next(self, blocking=None, timeout=0):
		""" Block and receive the next message """
		#Should not be reinitialized every time next is called
		#there is be leftover data which is otherwise get lost
		#incoming = MessageReader(self.infd, self.blocking)
		while True:
			msg = self.incoming.poll()
			if msg is not None:
				return msg.data  # Only MAGIMessages come in at this time

	def send(self, msg, **args):
		""" Block and send a message """
		outgoing = MessageSender(self.outfd, AgentRequest.MAGIMessage(msg, **args))
		while not outgoing.poll():
			pass

	def trigger(self, **args):
		'''Send a trigger to Magi message infrastructure.'''
		msg = APIMagiMessage(groups="control", docks="control", 
						  contenttype=APIMagiMessage.YAML, data=yaml.dump(args))
		self.send(msg)

	def joinGroup(self, group):
		""" Would like to see messages for group """
		outgoing = MessageSender(self.outfd, AgentRequest.JoinGroup(group))
		while not outgoing.poll():
			pass

	def leaveGroup(self, group):
		""" No longer care about messages for group, if another agent is still listening, the group will still be received """
		outgoing = MessageSender(self.outfd, AgentRequest.LeaveGroup(group))
		while not outgoing.poll():
			pass

	def listenDock(self, dock):
		""" Start listening for messages destined for 'dock' """
		outgoing = MessageSender(self.outfd, AgentRequest.ListenDock(dock))
		while not outgoing.poll():
			pass

	def unlistenDock(self, dock):
		""" Stop listening for messages destined for 'dock' """
		outgoing = MessageSender(self.outfd, AgentRequest.UnlistenDock(dock))
		while not outgoing.poll():
			pass

	def poisinPill(self):
		pass
#		""" queue a poisin pill so that anyone waiting on a call to next will wake up """
#		call = {'version': 1.0, 'method': 'poisinPill', 'args': {}}
#		stop_msg = APIMagiMessage(contenttype=APIMagiMessage.YAML, data=yaml.safe_dump(call))
#		outgoing = MessageSender(self.infd, AgentRequest.MAGIMessage(stop_msg))
#		while not outgoing.poll():
#			pass
