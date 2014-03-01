
import threading
import asyncore
import logging
import Queue
import time
import yaml

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
		self.invDockMap = defaultdict(set) # transport to list of dock names

		self.fromNetwork = Queue.Queue()
		self.messaging = messaging

		self.commPort = config.getConfig().get('processAgentsCommPort')
		if not self.commPort:
			self.commPort = 18809
			
		# Start a TCP server to listen from external agents
		self.server = TCPServer(address="127.0.0.1", port=self.commPort)
		# Add the server to the list of transports being polled
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
		for transportList in self.dockMap.values():
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
						dock = obj.data
						log.debug("%s requests listen %s" % (transport, dock))
						if hasattr(transport, "matchingPipe"):
							self.dockMap[dock].add(transport.matchingPipe)
							self.invDockMap[transport.matchingPipe].add(dock)
						else:
							self.dockMap[dock].add(transport)
							self.invDockMap[transport].add(dock)

					elif obj.request == AgentRequest.UNLISTEN_DOCK:
						dock = obj.data
						log.debug("%s requests unlisten %s" % (transport, dock))
						if hasattr(transport, "matchingPipe"):
							self.dockMap[dock].discard(transport.matchingPipe)
							if len(self.dockMap[dock]) == 0:
								del self.dockMap[dock]
							self.invDockMap[transport.matchingPipe].discard(dock)
							if len(self.invDockMap[transport.matchingPipe]) == 0:
								del self.invDockMap[transport.matchingPipe]
						else:
							self.dockMap[dock].discard(transport)
							if len(self.dockMap[dock]) == 0:
								del self.dockMap[dock]
							self.invDockMap[transport].discard(dock)
							if len(self.invDockMap[transport]) == 0:
								del self.invDockMap[transport]

					elif obj.request == AgentRequest.MESSAGE:
						log.debug("%s requests send message" % (transport))
						if hasattr(transport, "matchingPipe"):
							self.sendMessage(transport.matchingPipe, obj)
						else:
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
		if msg.srcdock is None and transport in self.invDockMap and len(self.invDockMap[transport]) > 0:
			msg.srcdock = list(self.invDockMap[transport])[0]  # default to first dock in list
			log.log(9, "Setting srcdock to %s", msg.srcdock)
		self.messaging.send(msg, **request.__dict__)

	def dispatchMessage(self, msg):
		""" A message came up from the daemon to be dispatched """
		log.debug("Dispatching message %s", msg)
		request = AgentRequest.MAGIMessage(msg)
		for dock in msg.dstdocks:
			for transport in self.dockMap[dock]:
				log.debug("enqueued to %s", transport)
				transport.outmessages.append(request)

	def wantsDock(self, dock):
		return dock in self.dockMap

	def unloadAll(self):
		""" Unload all the process agents 
		by sending a stop message on all the agent transports """
		log.info("Unloading all external agents")
		for transport, dockList in self.invDockMap.iteritems():
			call = {'version': 1.0, 'method': 'stop', 'args': {}}
			stop_msg = MAGIMessage(docks=list(dockList)[0], contenttype=MAGIMessage.YAML, 
										   data=yaml.safe_dump(call))
			request = AgentRequest.MAGIMessage(stop_msg)
			log.debug('Sending stop message on transport %s, dock %s', transport, list(dockList)[0])
			transport.outmessages.append(request)
		#Waiting for agents to unload
		for i in range(10):
			if len(self.invDockMap) == 0:
				log.debug("Agents unload done")
				break
			time.sleep(0.1) #waiting for the unload to be done
					
	def stop(self):
		""" Shutdown the TCP server and signal the main loop to exit """
		log.info("Stopping external agents manager thread")
		#Unloading all agents
		self.unloadAll()
#		del self.pollMap[self.server.fileno()]
		log.debug("Stopping server")
		if self.server:
			self.server.close() 
		log.debug("Server stopped")
		self.done = True

	def run(self):
		""" Called by thread main """
		self.threadId = config.getThreadId()
		log.info("External agents thread started. Thread id: " + str(self.threadId))
			
		self.done = False
		while not self.done:
			try:
				self.loop()
			except Exception, e:
				log.error("Failure in external agents thread: %s" % e, exc_info=True)
				log.debug("%s", self.pollMap) # Only convert to string on debug
				time.sleep(0.5) # Don't jump into a super loop on repeatable errors
				
		log.info("External agents thread stopped")
