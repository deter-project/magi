#!/usr/bin/env python

import unittest2

from magi.tests.util import *
from magi.orchestrator.parse import *

log = logging.getLogger(__name__)

class BranchingTest(unittest2.TestCase):
	"""
		Testing of handingstream branching in AAL Files.
	"""
	def test_countContraints(self):
		#TODO: Test needs to be fixed
		return
		
		args = {'count': 3}
		t = Trigger(**args)
		# TriggerData doesn't matter - we're only looking at count.
		incoming = TriggerData({'hello':'world'})
		print '\n'
		for i in range(args['count']):
			print t
			retVal = t.constraintMatched()
			self.assertEquals(retVal, False)
			t.update(incoming)

		print t
		retVal = t.constraintMatched()
		self.assertEquals(True, retVal)

	def test_nodeContraints(self):
		#TODO: Test needs to be fixed
		return
		
		nodeNames = ['dewey', 'screwem', 'howe']
		args = {'nodes': nodeNames}
		t = Trigger(**args)
		# TriggerData doesn't matter - we're only looking at count.
		print '\n'
		for name in nodeNames:
			print t
			retVal = t.constraintMatched()
			self.assertEquals(retVal, False)
			t.sets['nodes'].update([name])

		t.update({'dont': 'matter'})
		print t
		retVal = t.constraintMatched()
		self.assertEquals(True, retVal)

	def test_selfDestruct(self):
		#TODO: Test needs to be fixed
		return

		args = { 'event': 'Bork! Bork! Bork!' }
		t = Trigger(**args)
		print '\n'
		timeout = 3
		t.selfDestructTime=time.time() + float(timeout+2)
		for i in range(timeout): 
			self.assertEquals(t.shouldDelete(time.time()), False)
			print '.'
			time.sleep(1)

		print
		time.sleep(3)
		self.assertEquals(t.shouldDelete(time.time()), True)

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


