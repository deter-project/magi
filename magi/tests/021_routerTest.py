#!/usr/bin/env python

import unittest2
import Queue
import yaml
import logging
from magi.messaging.api import GroupRequest
from magi.messaging.worker import WorkerThread
from magi.messaging.magimessage import MAGIMessage
from magi.messaging.routerNode import NodeRouter
from magi.messaging.routerGroup import GroupRouter, listChecksum
from magi.tests.util import TestTransport


class RouterTest(unittest2.TestCase):
	"""
		Testing of router code in main socket loop of the messaging core
	"""

	def newMsg(self):
		msg = MAGIMessage()
		msg.msgid = 1234
		msg.flags = 0x0
		msg.contenttype = MAGIMessage.YAML
		msg.src = "mynode"
		msg.srcdock = "sourcedock"
		msg.hmac = "123456789"
		msg.dstnodes = ['n1', 'n2']
		msg.dstgroups = ['g1', 'g2']
		msg.dstdocks = ['d1', 'd2']
		msg.sequence = 98765
		msg.timestamp = 12347890
		msg.data = "helloworld"
		return msg


	def setUp(self):
		self.txqueue = Queue.Queue()
		self.rxqueue = Queue.Queue()
		self.router = WorkerThread("mynode", self.txqueue, self.rxqueue)
		self.transports = { 10 : TestTransport(10), 11 : TestTransport(11), 12 : TestTransport(12) }
		for key in self.transports:
			# Add transport to router and verify resend message is sent and then clear it
			self.router.addTransport(self.transports[key])
			self.router.loop()
			self.assertEquals(1, len(self.transports[key].outmessages))
			self.assertEquals(set([GroupRouter.ONEHOPNODES]), self.transports[key].outmessages[0].dstgroups)
			self.assertEquals(set([GroupRouter.DOCK]), self.transports[key].outmessages[0].dstdocks)
			self.transports[key].outmessages = []


	def push(self, msg):
		self.router.queues['PRE'].append(msg)
		self.router.loop()

	def commonAssert(self):
		""" Check that nothing makes it into data structures that are an unused deadend """
		self.assert_(len(self.router.transportMap[0].outmessages) == 0, "nothing should show up in local transport outmessages")
		self.assert_(len(self.router.transportMap[0].inmessages) == 0, "nothing should show up in local transport inmessages")


	def test_BasicRoute(self):
		""" Test basic functionality of routeMessage in router core"""
		# local to n1, route request out all external, response on t10, route out t10, then response on t11, paused messages send out t11
		rtmsg = MAGIMessage(src="n1", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg._receivedon = self.transports[10]
		rtmsg2 = MAGIMessage(src="n2", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg2._receivedon = self.transports[11]

		msg = self.newMsg()
		msg._receivedon = self.router.transportMap[0]

		self.push(rtmsg)
		self.push(msg)

		self._checkForMessageOnlyIn("data", yaml.safe_dump({'request':'n1'}), [])  # got response before msg, no requests
		self._checkForMessageOnlyIn("data", yaml.safe_dump({'request':'n2'}), [10,11,12]) # want to know about n2
		self._checkForMessageOnlyIn("msgid", 1234, [10])
		self.assert_(self.router.rxqueue.empty(), "Local receive queue should be empty")  # locally not n1,n2 or a member of g1 or g2
		self.commonAssert()

		self.router.transportMap[10].outmessages = [] # clear
		self.push(rtmsg2)
		self._checkForMessageOnlyIn("msgid", 1234, [11])


	def test_MultiRoute(self):
		""" Test multiple routes with loopback """
		# from local to g1, no route requests, should deliver out t11 for g1, t12 for g2, not back to local as that was source
		request = { 'add': ['g1'], 'count': 1, 'checksum': listChecksum(['g1']) }
		rtmsg1 = MAGIMessage(src="n1", groups=["__NEIGH__"], docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
		rtmsg1._receivedon = self.transports[11]

		request = { 'add': ['g2'], 'count': 1, 'checksum': listChecksum(['g2']) }
		rtmsg2 = MAGIMessage(src="n2", groups=["__NEIGH__"], docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
		rtmsg2._receivedon = self.transports[12]

		msg = self.newMsg()
		msg.dstnodes = []
		msg._receivedon = self.router.transportMap[0]

		self.router.routers[0].groupRequest(GroupRequest("join", "g1", "default"))
		self.push(rtmsg1)
		self.push(rtmsg2)
		self.push(msg)

		self._checkForMessageOnlyIn("dstdocks", ['RouteRequest'], [])
		self._checkForMessageOnlyIn("msgid", 1234, [11, 12])
		self.assertEquals(self.router.rxqueue.qsize(), 1, "Local receive queue should have an incoming message")
		self.commonAssert()


	def test_RouteBack(self):
		""" Test for no routing out same interface if routeresponse comes from there """
		# from n1 to n2,g1,g2 , send node request, get response on interface it came in out, don't send back that way, just drop
		msg = self.newMsg()
		msg.dstnodes = ['n2']
		msg._receivedon = self.transports[10]
	
		rtmsg = MAGIMessage(src="n2", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg._receivedon = self.transports[10]

		logging.disable(30) # disable warning for no route, expected
		self.push(msg)
		logging.disable(0)
		self.assertEquals(1, len(self.router.processors[6].pausedMessages['n2']))  # BIG DEPENDENCY ON PROCESSOR ORDERING in this test
		self.push(rtmsg)
		self.assertEquals(0, len(self.router.processors[6].pausedMessages['n2']))

		self._checkForMessageOnlyIn("data", yaml.safe_dump({'request':'n2'}), [11, 12])
		self._checkForMessageOnlyIn("msgid", 1234, [])
		self.assert_(self.router.rxqueue.empty(), "Local receive queue should be empty")  # routerdock message absorbed by router processor
		self.commonAssert()


	def test_DoubleSend(self):
		""" Test for no routing out same interface twice for paused messages """
		# from n1 to n2,g1, route requests on local, should deliver out t11 for g1,  and then out t11 again for n1 but be squelched
		msg = self.newMsg()
		msg.dstnodes = ['n2']
		msg._receivedon = self.router.transportMap[0]

		request = { 'add': ['g1'], 'count': 1, 'checksum': listChecksum(['g1']) }
		rtmsg1 = MAGIMessage(src="ignore", groups=["__NEIGH__"], docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
		rtmsg1._receivedon = self.transports[11]

		rtmsg2 = MAGIMessage(src="n2", groups=["__ALL__"], docks=[NodeRouter.DOCK], data=yaml.safe_dump({'response':True}))
		rtmsg2._receivedon = self.transports[11]

		self.push(rtmsg1)
		self.push(msg)
		self.push(rtmsg2)

		self._checkForMessageOnlyIn("data", yaml.safe_dump({'request':'n2'}), [10, 11, 12])
		self._checkForMessageOnlyIn("msgid", 1234, [11])
		msgcount = len(self.transports[11].outmessages)
		self.assert_(msgcount == 2, "Should only be 1 request and 1 message in t11 queue, found %d" % msgcount)
		self.assert_(self.router.rxqueue.empty(), "Local receive queue should be empty")
		self.commonAssert()


	def _checkForMessageOnlyIn(self, attr, val, queues):
		for fd, transport in self.router.transportMap.iteritems():
			if fd in queues:
				# Should find one
				found = False
				for msg in transport.outmessages:
					if getattr(msg, attr) == val:
						found = True
						break
				self.assert_(found, "%s=%s not found in %s" % (attr, val, transport))

			else:
				# Should not find one
				for msg in transport.outmessages:
					if getattr(msg, attr) == val:
						self.assert_(0, "%s=%s was found in %s" % (attr, val, transport))

if __name__ == '__main__':
	import logging
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(0) #logging.DEBUG)
	unittest2.main(verbosity=2)


