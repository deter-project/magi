
import logging
import time
import yaml
from collections import defaultdict
from magimessage import MAGIMessage
from processor import BlankRouter
from routerGroup import GroupRouter

from magi.util.Collection import namedtuple

log = logging.getLogger(__name__)
PausedMessage = namedtuple("PausedMessage", "msg, rxtransport")


class NodeEntry(object):
	__slots__ = ['fileno', 'touched']
	def __init__(self, fileno):
		self.update(fileno)

	def update(self, fileno):
		self.fileno = fileno
		self.touched = time.time()


class NodeRouter(BlankRouter):
	"""
		Maintains cache of nexthops for node names.
		When cache meets its max size, throw out items touched the longest time ago
		Also maintains a list of messages that are waiting for node routes
	"""
	MAXCACHESIZE = 1000
	DOCK = "__NODES__"

	def __init__(self):
		BlankRouter.__init__(self)
		self.nodeRouteCache = dict()
		self.inProcessRequests = dict()
		self.pausedMessages = defaultdict(list) 

	def cleanNodeCache(self, ratio):
		""" 
			Clean the cache of ratio% entries where 0 < ratio < 1.0.  Use an ordered list of access times to find out
			which access time to use as a breaking point for keeping entries
		"""
		times = [x.touched for x in self.nodeRouteCache.itervalues()]
		times.sort()
		median = times[len(times)*ratio]
		self.nodeRouteCache = dict([(src, entry) for src, entry in self.nodeRouteCache.iteritems() if entry.touched > median])


	def processIN(self, msglist, now):
		"""
			Incoming message destined for this node.  Returns None if we absorb the message.
		"""
		passed = list()
		for msg in msglist:
			if NodeRouter.DOCK not in msg.dstdocks:
				if msg.src in self.pausedMessages:
					# Use the information we have at hand to allow paused messages to go out
					self.processRouteResponse(msg)
				passed.append(msg)
				continue
	
			nodemessage = yaml.load(msg.data)
			if 'request' in nodemessage:
				self.processRouteRequest(nodemessage['request'])
			elif 'response' in nodemessage:
				self.processRouteResponse(msg)
	
		return passed


	def processRouteResponse(self, msg):
		"""
			Process a response from a node for its location
		"""
		log.debug("Processing response from '%s' for its location", msg.src)
		if msg.src == self.nodename:
			# As we allow loopback, this can happen, just ignore it
			return

		# already have it, update and return
		if msg.src in self.nodeRouteCache:
			self.nodeRouteCache[msg.src].update(msg._receivedon.fileno())
			return 

		# new node info, create a new entry, cleaning cache if necessary
		if len(self.nodeRouteCache) >= self.MAXCACHESIZE:
			self.cleanNodeCache(0.25)
		if msg._receivedon is None:
			log.error("Can't process response from %s, no received on interface", msg.src)
		else:
			log.debug("Adding route to cache: %s -> %s", msg.src, msg._receivedon.fileno())
			self.nodeRouteCache[msg.src] = NodeEntry(msg._receivedon.fileno())

		# Check for messages waiting for route and queue
		waiting = self.pausedMessages.get(msg.src, [])
		for paused in waiting:
			if paused._receivedon is msg._receivedon:
				log.debug("Not sending paused message for %s out %d, same as rx interface", msg.src, msg._receivedon.fileno())
			elif msg._receivedon.fileno() in paused._appendedto:
				log.debug("Not sending paused message for %s out %d, already went out for something else", msg.src, msg._receivedon.fileno())
			else:
				log.debug("sending paused message for: %s", msg.src)
				paused._routed = [msg._receivedon.fileno()]
				self.msgintf.sendDirect(paused)

		if len(waiting) > 0:
			self.pausedMessages[msg.src] = []


	def processRouteRequest(self, nodename):
		"""
			Process a request for a node route
		"""
		# TODO: Should we cache src now?
		log.debug("Processing route request")
		if nodename.strip() == self.nodename:
			resp = MAGIMessage(contenttype=MAGIMessage.YAML, docks=[NodeRouter.DOCK], groups=[GroupRouter.ALLNODES], data=yaml.safe_dump({'response':True}))
			self.msgintf.send(resp)


	def requestRoute(self, intransport, node):
		"""
			Need a route, see if we should sent a request and if so, do it
		"""
		log.debug("Requesting route for %s", node)
		reqt = self.inProcessRequests.get(node, 0)
		now = time.time()
		if reqt + 10 > now:
			log.info("Squelch route request for %s as there is one in process", node)
			return
			
		self.inProcessRequests[node] = now
		# Send a request out all interfaces except local and receiving interface
		req = MAGIMessage(contenttype=MAGIMessage.BLOB, docks=[NodeRouter.DOCK], groups=[GroupRouter.ALLNODES], data=yaml.safe_dump({'request':node}))
		req._routed = set(self.transports.keys()) - set([0])
		self.msgintf.send(req)


	def routeMessage(self, msg):
		""" Return a list of all the transport filenos this message should be sent out based on node names """
		ret = set()
		log.debug("Routing message to destination nodes: %s", msg.dstnodes)
		for node in msg.dstnodes:
			if node == self.nodename:
				ret.add(0)
				continue

			if node in self.nodeRouteCache:
				ret.add(self.nodeRouteCache[node].fileno)
				continue

			# Need to send RouteRequest and pause message for this node
			self.pausedMessages[node].append(msg)
			self.requestRoute(msg._receivedon, node)
		log.debug("Message routed on transports: %s", ret)
		return ret

