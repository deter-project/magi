#!/usr/bin/env python

from collections import defaultdict
from magi.orchestrator.parse import createTrigger
import logging
import time
import unittest2

log = logging.getLogger(__name__)

class BranchingTest(unittest2.TestCase):
	"""
		Testing of handling stream branching in AAL Files.
	"""
	
	def cacheTrigger(self, triggerCache, incoming):
		merged = False
		for trigger in triggerCache[incoming.event]:
			if trigger.isEqual(incoming):
				trigger.merge(incoming)
				merged = True
				break
		if not merged:
			triggerCache[incoming.event].append(incoming)

			
	def test_countContraints(self):
		
		triggerCache = defaultdict(list)
		
		# TriggerData doesn't matter - we're only looking at count.
		t = createTrigger({'event' : 'CountTest', 'count': 3})
		t.activate()
		
		for i in range(t.count):
			incoming = createTrigger({'event' : 'CountTest', 'nodes' : i })
			retVal = t.isComplete(triggerCache)
			self.assertEquals(retVal, False)
			self.cacheTrigger(triggerCache, incoming)

		retVal = t.isComplete(triggerCache)
		self.assertEquals(True, retVal)

	def test_nodeContraints(self):
		
		triggerCache = defaultdict(list)
		
		# TriggerData doesn't matter - we're only looking at count.
		nodeNames = ['dewey', 'screwem', 'howe']
		args = {'event' : 'CountTest', 'nodes' : nodeNames}
		t = createTrigger(args)
		t.activate()
		
		incoming = createTrigger({'event' : 'CountTest', 'nodes' : 'somenode'})
		self.cacheTrigger(triggerCache, incoming)
		
		incoming = createTrigger({'event' : 'CountTest', 'nodes' : 'somenode1'})
		self.cacheTrigger(triggerCache, incoming)
		
		for name in nodeNames:
			incoming = createTrigger({'event' : 'CountTest', 'nodes' : name})
			retVal = t.isComplete(triggerCache)
			self.assertEquals(retVal, False)
			self.cacheTrigger(triggerCache, incoming)

		retVal = t.isComplete(triggerCache)
		self.assertEquals(True, retVal)

	def test_logicalTriggers(self):
		
		triggerCache = defaultdict(list)
		
		td1 = {'event' : 'TestTrigger'}
		t1 = createTrigger(td1)
		t1.activate()
		
		incoming = createTrigger({'event' : 'TestTrigger', 'nodes' : 'node'})
		self.cacheTrigger(triggerCache, incoming)
		
		retVal = t1.isComplete(triggerCache)
		self.assertEquals(retVal, True)
		
		log.debug('t1 -> complete')
		
		td2 = {'timeout' : 2000}
		t2 = createTrigger(td2)
		t2.activate()
		self.assertEquals(t2.isComplete(triggerCache), False)
		
		log.debug('t2 -> not complete')
		
		td3 = {'type' : 'AND', 'triggers' : [td1, td2]}
		t3 = createTrigger(td3)
		t3.activate()
		self.assertEquals(t3.isComplete(triggerCache), False)
		
		log.debug('t1 and t2 -> not complete')
		
		td4 = {'type' : 'OR', 'triggers' : [td1, td2]}
		t4 = createTrigger(td4)
		t4.activate()
		self.assertEquals(t4.isComplete(triggerCache), True)
		
		log.debug('t1 or t2 -> complete')
		
		time.sleep(2)
		
		log.debug('sleep for 2 seconds')
		
		self.assertEquals(t1.isComplete(triggerCache), True)
		self.assertEquals(t2.isComplete(triggerCache), True)
		self.assertEquals(t3.isComplete(triggerCache), True)
		self.assertEquals(t4.isComplete(triggerCache), True)
		
		log.debug('all complete')
		
	def test_disjunctionTriggers(self):
		
		triggerCache = defaultdict(list)
		
		td0 = {}
		t0 = createTrigger(td0)
		t0.activate()
		retVal = t0.isComplete(triggerCache)
		self.assertEquals(retVal, True)
		
		td1 = {'event' : 'TestTrigger'}
		t1 = createTrigger(td1)
		t1.activate()
		retVal = t1.isComplete(triggerCache)
		self.assertEquals(retVal, False)
		
		td2 = {'type' : 'OR', 'triggers' : [td0, td1]}
		t2 = createTrigger(td2)
		t2.activate()
		retVal = t2.isComplete(triggerCache)
		self.assertEquals(retVal, True)
		
		td3 = {'type' : 'OR', 'triggers' : [td1, td2]}
		t3 = createTrigger(td3)
		t3.activate()
		retVal = t3.isComplete(triggerCache)
		self.assertEquals(retVal, True)
		
		td4 = {'type' : 'OR', 'triggers' : [td1, td3]}
		t4 = createTrigger(td4)
		t4.activate()
		retVal = t4.isComplete(triggerCache)
		self.assertEquals(retVal, True)
		
	def test_conjunctionTriggers(self):

		triggerCache = defaultdict(list)
		
		td0 = {}
		t0 = createTrigger(td0)
		t0.activate()
		retVal = t0.isComplete(triggerCache)
		self.assertEquals(retVal, True)
		
		td1 = {'event' : 'TestTrigger'}
		t1 = createTrigger(td1)
		t1.activate()
		retVal = t1.isComplete(triggerCache)
		self.assertEquals(retVal, False)
		
		td2 = {'type' : 'AND', 'triggers' : [td0, td1]}
		t2 = createTrigger(td2)
		t2.activate()
		retVal = t2.isComplete(triggerCache)
		self.assertEquals(retVal, False)
		
		td3 = {'type' : 'AND', 'triggers' : [td0, td2]}
		t3 = createTrigger(td3)
		t3.activate()
		retVal = t3.isComplete(triggerCache)
		self.assertEquals(retVal, False)
		
		td4 = {'type' : 'AND', 'triggers' : [td0, td3]}
		t4 = createTrigger(td4)
		t4.activate()
		retVal = t4.isComplete(triggerCache)
		self.assertEquals(retVal, False)

#	def test_selfDestruct(self):
#
#		args = { 'event': 'Bork! Bork! Bork!' }
#		t = Trigger(**args)
#		timeout = 3
#		t.selfDestructTime=time.time() + float(timeout+2)
#		for i in range(timeout): 
#			self.assertEquals(t.shouldDelete(time.time()), False)
#			time.sleep(1)
#
#		time.sleep(3)
#		self.assertEquals(t.shouldDelete(time.time()), True)

if __name__ == '__main__':
	import signal
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)


