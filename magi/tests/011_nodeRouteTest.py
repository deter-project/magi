#!/usr/bin/env python

import unittest2
import logging
import yaml
import time
from magi.tests.util import TestTransport, TestMessageIntf
from magi.messaging.magimessage import MAGIMessage
from magi.messaging.routerNode import NodeRouter

class NodeRouteTest(unittest2.TestCase):

	def setUp(self):
		self.transports = { 0:TestTransport(0), 1:TestTransport(1), 2:TestTransport(2) }
		self.store = TestMessageIntf()
		self.router = NodeRouter()
		self.router.configure(name="mynode", transports=self.transports, msgintf=self.store)

	def tearDown(self):
		self.router = None

	def test_routeUnknown(self):
		""" Test route without a destination node  """
		msg = MAGIMessage(src="thesrc", nodes=["unknown"])
		msg.msgid = 12345
		msg._receivedon = self.transports[1]

		rtmsg = MAGIMessage(src="unknown", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg._receivedon = self.transports[2]

		# Attempt to route message, should have nowhere to go
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([]))
		self._checkForMessageOnlyIn('data', yaml.safe_dump({'request':'unknown'}), [2])

		# Get a route response and check for proper queing, original header information should stay intact when sending paused
		self.router.processIN([rtmsg], time.time())
		self._checkForMessageOnlyIn('dstnodes', set(['unknown']), [2])
		self._checkForMessageOnlyIn('src', "thesrc", [2])
		self._checkForMessageOnlyIn('msgid', 12345, [2])

	def test_routeNoBackwards(self):
		""" Test route and request without sending back out initial receiving interface """
		msg = MAGIMessage(nodes=["unknown"])
		msg._receivedon = self.transports[1]

		rtmsg = MAGIMessage(src="unknown", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg._receivedon = self.transports[1]

		# Attempt to route message, should have nowhere to go
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([]))
		self._checkForMessageOnlyIn('data', yaml.safe_dump({'request':'unknown'}), [2])

		# Get a route response and check for proper queing
		self.router.processIN([rtmsg], time.time())
		self._checkForMessageOnlyIn('dstnodes', ['unknown'], [])

	def test_requestSquelch(self):
		""" Test to make sure squelching of requests occurs """
		msg1 = MAGIMessage(nodes=["unknown"], data='1')
		msg1._receivedon = self.transports[1]

		msg2 = MAGIMessage(nodes=["unknown"], data='2')
		msg2._receivedon = self.transports[1]

		# Two messages for unknown, only one request should be queued
		fds = self.router.routeMessage(msg1)
		self.assertEqual(fds, set([]))
		fds = self.router.routeMessage(msg2)
		self.assertEqual(fds, set([]))
		self._checkForMessageOnlyIn('data', yaml.safe_dump({'request':'unknown'}), [2])
		self._checkMessagesInEquals(1, 0)
		self._checkMessagesInEquals(2, 1)

	def test_multipleRoutes(self):
		""" Test for multiple nodes including localhost as one """
		msg = MAGIMessage(nodes=["unknown","mynode"])
		msg._receivedon = self.transports[2]

		rtmsg = MAGIMessage(src="unknown", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg._receivedon = self.transports[1]

		self.router.processIN([rtmsg], time.time())
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([0,1]))


	def test_RequestResponse(self):
		""" Test basic response to a request """
		rtmsg = MAGIMessage(src="unknown", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'request':'mynode'}))
		rtmsg._receivedon = self.transports[1]
		self.router.processIN([rtmsg], time.time())
		self.assertEquals(1, len(self.store.outgoing))
		self.assertEquals(set([NodeRouter.DOCK]), self.store.outgoing[0].dstdocks)
		self.assert_('response' in yaml.load(self.store.outgoing[0].data))


	def _checkForMessageOnlyIn(self, attr, val, queues):
		#for fd, transport in self.transports.iteritems():
		found = False
		for msg in self.store.outgoing:
			if getattr(msg, attr) == val:
				self.assert_(queues == list(msg._routed), "queue lists are not correct %s while expected %s" % (msg._routed, queues))
				found = True

		if len(queues) > 0:
			self.assert_(found, "%s=%s not found anywhere, should have been in %s" % (attr, val, queues))
			
	def _checkMessagesInEquals(self, queue, count):
		# Check message set up for transmit and count based on which transports they are scheduled for transmission on
		tcount = 0
		for msg in self.store.outgoing:
			if queue in msg._routed:
				tcount += 1
		self.assert_(count == tcount, "queue counts are not correct %s while expected %s" % (tcount, count))


if __name__ == '__main__':
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)

