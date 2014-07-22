#!/usr/bin/env python

import unittest2
import logging
import time
from magi.messaging.api import *

class MessagingCompleteTest(unittest2.TestCase):
	"""
		Testing of messaging as seens through the API
	"""

	def setUp(self):
		self.messaging = Messenger("myname")
#		self.messaging.startDaemon()

	def tearDown(self):
		pass

	def testJoinLeave(self):
		""" Test join/leave requests with full messenger setup """
		self.messaging.join("somegroup")
		time.sleep(0.2)
		self.messaging.leave("somegroup")
		time.sleep(0.2)

	def testTrigger(self):
		self.messaging.trigger(event="myevent", special1="special2", nodes=['1', '2', '3'])
		time.sleep(0.1)


if __name__ == '__main__':
	import signal
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(0) #logging.DEBUG)
	unittest2.main(verbosity=2)

