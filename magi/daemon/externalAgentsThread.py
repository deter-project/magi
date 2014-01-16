
import threading
import asyncore
import logging
import Queue
import time

from collections import defaultdict
from magi.messaging.api import MAGIMessage, TCPServer, TCPTransport, DefaultCodec
from magi.util.Collection import namedtuple
from magi.daemon.processInterface import AgentCodec, AgentRequest
from magi.util import config

log = logging.getLogger(__name__)

PipeTuple = namedtuple("PipeTuple", "docklist, incoming, outgoing")

class ExternalAgentsThread(threading.Thread):
	"""
		Another async thread for sending messages in and out of pipes or sockets connected to processes acting as agents.
		It also processes the server connection inputs.
		Messaging coming from the agents travel from here directly into the messaging system.

		pollMap: fd -> Transport (Client, Server, PipeIn, PipeOut)
		dockMap: dock -> List of Transports
		invDockMap: Transport -> List of Docks
		
		pipeTransport.matchingPipe contains opposite pipeTransport

	"""
		
	def __init__(self, messaging):
		threading.Thread.__init__(self, name='extagents')
		self.setDaemon(True) # This thread won't keep python interpreter running when everything else stops
		self.msgCodec = DefaultCodec()
		self.agentCodec = AgentCodec()

		self.pollMap = dict() # fd to Pipes, Sockets and server
		self.dockMap = defaultdict(set) # dock to list of writable streams
		self.invDockMap = defaultdict(list) # transport to list of dock names

		self.fromNetwork = Queue.Queue()
		self.messaging = messaging

		self.commPort = config.getConfig().get('processAgentsCommPort')
		if not self.commPort:
			self.commPort = 18809
		
		# Start and add the TCP server
		self.server = TCPServer(address="127.0.0.1", port=self.commPort)
		self.pollMap[self.server.fileno()] = self.server


	def clearMaps(self, fd, transport):
		"""
			Attempt to clear any links to this old transport from our maps.  Note that transport.fileno() may no longer
			be valid, so we pass fd in with the transport as well.
		"""
		if fd in self.pollMap:
			del self.pollMap[fd]
		else:
			log.info("%s not found in pollmap", fd)

		if transport in self.invDockMap:
			del self.invDockMap[transport] 
		for name, transportList in self.dockMap.iteritems():
			transportList.discard(transport)


	def loop(self):
		if len(self.pollMap) <= 0:
			time.sleep(0.5)
		else:
			asyncore.poll(0.1, self.pollMap)

		# Check for and deal with closed transports
		for fd, transport in self.pollMap.items():
			if getattr(transport, 'closed', False):
				log.info("%s closed", transport)
				self.clearMaps(fd, transport)
				if hasattr(transport, "matchingPipe"):
					transport.matchingPipe.handle_close()
					self.clearMaps(transport.matchingPipe.fileno(), transport.matchingPipe)

		# Process received messages from agents
		for fd, transport in self.pollMap.items():
			if len(transport.inmessages) > 0:
				log.debug("%d messages from %s", len(transport.inmessages), transport)

			for obj in transport.inmessages:
				if isinstance(obj, TCPTransport):
					log.info("New TCP connection made from an agent: %s", obj)
					self.pollMap[obj.fileno()] = obj  # docks come later in requests
					obj.setCodec(self.agentCodec)
	
				elif isinstance(obj, AgentRequest):
					# Fill in necessary components and send
					if obj.request == AgentRequest.JOIN_GROUP:
						log.debug("%s requests join %s" % (transport, obj.data))
						self.messaging.join(obj.data, str(transport)) # 'transport' joins

					elif obj.request == AgentRequest.LEAVE_GROUP:
						log.debug("%s requests leave %s" % (transport, obj.data))
						self.messaing.leave(obj.data, str(transport)) # 'transport' leaves

					elif obj.request == AgentRequest.LISTEN_DOCK:
						log.debug("%s requests listen %s" % (transport, obj.data))
						if hasattr(transport, "matchingPipe"):
							self.dockMap[obj.data].add(transport.matchingPipe)
						else:
							self.dockMap[obj.data].add(transport)
						self.invDockMap[transport].append(obj.data)

					elif obj.request == AgentRequest.LEAVE_GROUP:
						log.debug("%s requests unlisten %s" % (transport, obj.data))
						if hasattr(transport, "matchingPipe"):
							self.dockMap[obj.data].discard(transport.matchingPipe)
						else:
							self.dockMap[obj.data].discard(transport)
						if obj.data in self.invDockMap[transport]:
							self.invDockMap[transport].remove(obj.data)

					elif obj.request == AgentRequest.MESSAGE:
						log.debug("%s requests send message" % (transport))
						self.sendMessage(transport, obj)	

					else:
						log.error("Unknown request from agent.  Type = '%s'", obj.request)
	
			transport.inmessages = []


		# Process requests from the daemon
		while not self.fromNetwork.empty():
			obj = self.fromNetwork.get_nowait()
			if isinstance(obj, MAGIMessage):
				self.dispatchMessage(obj)
			elif isinstance(obj, PipeTuple):
				self.addPipe(obj)


	def addPipe(self, info):
		"""  A new pipe (in transport and out transport) is to be added to your list of active descriptors """
		log.info("Request to add new pipe %s to map", info)
		info.incoming.matchingPipe = info.outgoing  # match the two together
		info.incoming.setCodec(self.agentCodec)
		info.outgoing.matchingPipe = info.incoming
		info.outgoing.setCodec(self.agentCodec)

		#self.dockMap[info.dock].add(info.outgoing)  # docks wait for listen request from agent
		self.pollMap[info.incoming.fileno()] = info.incoming
		self.pollMap[info.outgoing.fileno()] = info.outgoing


	def sendMessage(self, transport, request):
		""" The agent requested that the following message be sent """
		msg, hdrsize = self.msgCodec.decode(request.data)
		msg.data = request.data[hdrsize:]
		if msg.srcdock is None and len(self.invDockMap[transport]) > 0:
			msg.srcdock = self.invDockMap[transport][0]  # default to first dock in list
			log.log(9, "Setting srcdock to %s", msg.srcdock)
		self.messaging.send(msg, **request.__dict__)

	def dispatchMessage(self, msg):
		""" A message came up from the daemon to be dispatched """
		log.debug("Dispatching message %s", msg)
		request = AgentRequest.MAGIMessage(msg)
		for dock in msg.dstdocks:
			for agent in self.dockMap[dock]:
				log.debug("enqueued to %s", agent)
				agent.outmessages.append(request)

	def wantsDock(self, dock):
		return dock in self.dockMap

	def stop(self):
		""" Shutdown the TCP server and signal the main loop to exit """
		log.debug("starting process thread stop")
		del self.pollMap[self.server.fileno()]
		self.done = True
		time.sleep(0.1)
		self.server.close() 
		log.debug("Done with process thread stop")

	def run(self):
		""" Called by thread main """
		self.done = False
		while not self.done:
			try:
				self.loop()
			except Exception, e:
				log.error("Failure in pipe thread: %s" % e, exc_info=True)
				log.debug("%s", self.pollMap) # Only convert to string on debug
				time.sleep(0.5) # Don't jump into a super loop on repeatable errors




