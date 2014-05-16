from magi.messaging.api import DefaultCodec, MAGIMessage as APIMagiMessage

import Queue
import asyncore
import struct
import threading
import yaml
import logging

log = logging.getLogger(__name__)

class AgentRequest(object):
	"""
		Object form of process agent requests
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
		Codec for encoding and decoding messages on the process agent messaging interface.
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
		log.debug("Decoding AgentRequest message")
		
		newmsg = AgentRequest()
		(totallen, hdrlen, newmsg.request) = struct.unpack('>IHB', headerbuf[0:7])
		
		log.debug("totallen: %d", totallen)
		log.debug("hdrlen: %d", hdrlen)
		log.debug("newmsg.request: %d", newmsg.request)

		idx = 7
		while idx < hdrlen+6:
			(htype, hlen) = struct.unpack('>BB', headerbuf[idx:idx+2])
			idx += 2
			hname = AgentRequest.OPTIONS.get(htype, None)
			log.debug("setting option %s", hname)

			if hname is None: 
				log.warning("Don't understand header option %d, skipping", htype)
			elif htype == 3:
				setattr(newmsg, hname, struct.unpack('>I', headerbuf[idx:idx+hlen])[0])
			else:
				setattr(newmsg, hname, True)
			idx += hlen

		return newmsg, hdrlen+6


class AgentMessenger(threading.Thread):
	""" The messaging interface for an external process agent. """

	def __init__(self, inTransport, outTransport, agent):
		threading.Thread.__init__(self, name=agent.name+"_messenger")
		self.daemon = True
		self.agent = agent
		
		self.codec = AgentCodec()
		self.msgCodec = DefaultCodec()
		self.inqueue = Queue.Queue()

		self.inTransport = inTransport
		self.outTransport = outTransport
		self.inTransport.setCodec(self.codec)
		self.outTransport.setCodec(self.codec)
		
		self.pollMap = dict()
		
	def run(self):
		log.info("Running messenger for agent " + self.agent.name)

		self.pollMap[self.inTransport.fileno()] = self.inTransport
		self.pollMap[self.outTransport.fileno()] = self.outTransport
		
		while not self.agent.done:
			asyncore.poll(0.1, self.pollMap)
			if len(self.inTransport.inmessages) > 0:
				log.debug("%d messages from %s", len(self.inTransport.inmessages), self.inTransport)
				for msg in self.inTransport.inmessages:
					self.inqueue.put(msg)
				self.inTransport.inmessages = []
				
		while len(self.outTransport.outmessages) > 0:
			asyncore.poll(0, self.pollMap)
			
		log.info("Stopping messenger for agent " + self.agent.name)
	
	def next(self, block=True, timeout=None):
		""" Received the next message """
		msg = self.inqueue.get(block, timeout)
		
		# If its a MAGIMessage, decode now
		# Only MAGIMessages come in at this time
		if msg.request == AgentRequest.MESSAGE:  
			log.debug("Decoding received message")
			magimsg, hdrsize = self.msgCodec.decode(msg.data)
			log.debug("decoded message portion (%s, %s)", hdrsize, magimsg)
			magimsg.data = msg.data[hdrsize:]
			log.debug("Received Message: %s", magimsg)
			return magimsg
		
		log.debug("Non magi message request")
		log.debug(msg)
		return msg
		
	def send(self, msg, **args):
		""" Block and send a message """
		log.debug("Sending msg: %s", msg)
		request = AgentRequest.MAGIMessage(msg, **args)
		self.outTransport.outmessages.append(request)

	def trigger(self, **args):
		'''Send a trigger to Magi message infrastructure.'''
		msg = APIMagiMessage(groups="control", docks="control", 
						  contenttype=APIMagiMessage.YAML, data=yaml.dump(args))
		self.send(msg)

	def joinGroup(self, group):
		""" Would like to see messages for group """
		request = AgentRequest.JoinGroup(group)
		self.outTransport.outmessages.append(request)

	def leaveGroup(self, group):
		""" No longer care about messages for group """
		request = AgentRequest.LeaveGroup(group)
		self.outTransport.outmessages.append(request)

	def listenDock(self, dock):
		""" Start listening for messages destined for 'dock' """
		request = AgentRequest.ListenDock(dock)
		self.outTransport.outmessages.append(request)

	def unlistenDock(self, dock):
		""" Stop listening for messages destined for 'dock' """
		request = AgentRequest.UnlistenDock(dock)
		self.outTransport.outmessages.append(request)

	def poisinPill(self):
		pass
		""" queue a poisin pill so that anyone waiting on a call to next will wake up """
		call = {'version': 1.0, 'method': 'poisinPill', 'args': {}}
		stop_msg = APIMagiMessage(contenttype=APIMagiMessage.YAML, data=yaml.safe_dump(call))
		self.inqueue.put(stop_msg)
