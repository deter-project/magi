#!/usr/bin/env python

from magi.testbed import testbed
from magi.util import database
from magi.util.agent import ReportingDispatchAgent, agentmethod
from magi.util.processAgent import initializeProcessAgent
from magi.util.software import requireSoftware
from pktcounters import IPTablesCounters, IPFWCounters, NPFCounters, Counters

import logging
import sys

log = logging.getLogger(__name__)

class CounterRecorder(ReportingDispatchAgent):
	""" Class to read counter data via appropriate counter object and report it periodically """

	def __init__(self):
		ReportingDispatchAgent.__init__(self)
		self.active = False
		self.interval = 1
		self.filters = dict()

		if sys.platform.startswith('linux'):
			self.counters = IPTablesCounters()
		elif sys.platform.startswith('freebsd'):
			self.counters = IPFWCounters()
		elif sys.platform.startswith('cygwin'):
			requireSoftware('winstat')
			self.counters = NPFCounters()
		else:
			self.counters = Counters()
			
		#self.populateIntf2Node()
		self.truncate = True

#	def stop(self, msg):
#		""" Override agent loop so we can clear counters """
#		self.counters.clear()
#		ReportingDispatchAgent.stop(self, msg)

	def periodic(self, now):
		""" Actually read the counter data at this point in time """
		log.debug("Entering periodic()")
		
		if not self.active:
			return int(now + 1) - now  # round off to next second

		try:
			# Take data from the counter reader and put it into storage
			log.debug("Loading Counters")
			data = self.counters.loadCounters()
			log.debug("Clearing Counters")
			self.clearCounters(None)
			log.debug("Setting default Counters")
			self.setDefaults(None)
			
			log.debug("Fetched Counters")
			log.debug(data)
			
			log.debug("Collecting sensed counters")
			tinBytes, tinPackets, toutBytes, toutPackets = 0, 0, 0, 0
			for (name, counters) in data.iteritems():
				# Get the current counters, make sure their ints, create a new entry and store
				(pkts, bytes) = map(int, counters)
				if name.startswith('in'):
					tinBytes += bytes
					tinPackets += pkts
				elif name.startswith('out'):
					toutBytes += bytes
					toutPackets += pkts
				
				(trafficDirection, intfName) = name.split('@')
				self.collection.insert({"intfName" : intfName, "peerNode" : "peernode", "trafficDirection": trafficDirection, "packets" : pkts, "bytes" : bytes})	

			if tinPackets:
				self.collection.insert({"intfName" : "total", "trafficDirection": "in", "packets" : tinPackets, "bytes" : tinBytes})

			if toutPackets:
				self.collection.insert({"intfName" : "total", "trafficDirection": "out", "packets" : toutPackets, "bytes" : toutBytes})

		except Exception:
			log.error("Problem reading counters", exc_info=1)
			
		log.debug("Exiting periodic()")
		return int(now + self.interval) - now  # round off to next second

	def sense(self, msg, peerNode, trafficDirection, thresholdLow, thresholdHigh):
		""" Count packets going out of a given interface """
		log.info("Sensing data")
		
		result = 0
		try:
			itr = self.collection.find({'peerNode' : peerNode, 'trafficDirection' : trafficDirection }).sort('created', -1)
			rec = itr.next()
			bytes = rec['bytes']
			log.info("Peer Node: %s, Traffic Direction: %s, Bytes: %d", peerNode, trafficDirection, bytes)
			
			if bytes < thresholdLow:
				result = -1
			elif bytes > thresholdHigh:
				result = 1
			else:
				result = 0
		except Exception:
			log.error("Problem reading data", exc_info=1)
						
		log.info(result)
		return {'result': result}

	def populateIntf2Node(self):
		self.intf2NodeMap = dict()
		for intf in testbed.getInterfaceList():
			self.intf2NodeMap[intf.name] = self.intf2Node(intf.name)

	def intf2Node(self, intf):
		try:
			ip = testbed.getInterfaceInfo(matchname=intf).ip
		except Exception:
			raise Exception("No information available for interface. Mostly invalid interface. Interface Name: %s" %(intf))
		
		topoGraph = testbed.getTopoGraph()
		src = testbed.getNodeName()
		
		for link in topoGraph.node[src]['links'].values():
			if ip == link['ip']:
				linkName = link['name']
				break
			
		if not linkName:
			raise Exception("No information available for interface. Mostly invalid interface. Interface Name: %s" %(intf))
		
		neighbors = topoGraph.neighbors(src);
		neighbors.sort()
		for neighbor in neighbors:
			if linkName == topoGraph[src][neighbor]['linkName']:
				return neighbor
			
		raise Exception("No information available for interface. Mostly invalid interface. Interface Name: %s" %(intf))

	@agentmethod()
	def startCollection(self, msg):
		self.setDefaults(None)
		self.collection = database.getCollection(self.name)
		for name, filtermap in self.filters.iteritems():
			self.counters.addDataCounter(name, **filtermap)
		self.active = True
		if self.truncate:
			log.debug("truncating old records")
			self.collection.remove()
		return True

	@agentmethod()
	def stopCollection(self, msg):
		self.active = False
		#self.counters.clear()
		return True

	@agentmethod()
	def setDefaults(self, msg):  
		""" shortcut to add default counters similar to old 1.6 MAGI """
		#for intf in testbed.getInterfaceList():
			#self.addCounter(None, "in-"+intf.ip, {'input':intf.ip})
			#self.addCounter(None, "out-"+intf.ip, {'output':intf.ip})
		self.addCounter(None, "in@h1-eth0" , {'input':'h1-eth0'})
		self.addCounter(None, "out@h1-eth0" , {'output':'h1-eth0'})

		return True

	# Old things to save
	@agentmethod()
	def removeDefaults(self, msg):  
		""" shortcut to remove default counters added via addDefaults """
		for intf in testbed.getInterfaceList():
			self.removeCounter(None, "in-%s" %(intf.ip))
			self.removeCounter(None, "out-%s" %(intf.ip))

		return True

	@agentmethod()
	def addCounter(self, msg, name, filtermap):
		""" Request to add a new counter to monitor """
		self.counters.addDataCounter(name, **filtermap)
		return True

	@agentmethod()
	def removeCounter(self, msg, name):
		""" Request to remove a monitored counter """
		self.counters.delDataCounter(name)
		return True

	@agentmethod()
	def clearCounters(self, msg):
		""" Request to remove all monitored counters """
		self.counters.clear()
		return True


def getAgent(**kwargs):
	""" Required by module loader to get an instance of an agent """
	agent = CounterRecorder()
	agent.setConfiguration(None, **kwargs)
	return agent

if __name__ == "__main__":
	agent = CounterRecorder()
	kwargs = initializeProcessAgent(agent, sys.argv)
	agent.setConfiguration(None, **kwargs)
	agent.run()
