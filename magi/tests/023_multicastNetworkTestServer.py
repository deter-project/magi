#!/usr/bin/env python

import unittest2
import logging
import time

from magi.messaging.magimessage import MAGIMessage
from magi.messaging.transportMulticast import MulticastTransport
from magi.testbed import testbed
from magi.messaging.api import Messenger


class TransportTest(unittest2.TestCase):
	"""
		Testing of basics in TCPTransport class
	"""

	def setUp(self):
		self.messenger = Messenger("testmessenger")
		self.conn = MulticastTransport('239.255.1.1', 18808, testbed.controlip)
		self.messenger.addTransport(self.conn, True)
		self.messenger.join('multicastgroup', 'tester')
		self.msgid = 1234

	def sendMsg(self):
		self.msgid += 1
		msg = MAGIMessage()
		msg.msgid = self.msgid
		msg.contenttype = MAGIMessage.NONE
		msg.src = "servernode"
		msg.srcdock = "serverdock"
		msg.dstgroups = ['multicastgroup']
		msg.data = "success"
		msg._routed = [self.conn.fileno()]
		self.messenger.thread.sendDirect(msg)
		while self.messenger.thread.pollMap[self.conn.fileno()].outmessages:
			time.sleep(0.2) #waiting for message to be sent
		
	def test_BasicRequest(self):
		""" Testing multicast transport - Server """
		pass
#		msg = self.messenger.nextMessage(block=True)
#		self.assertEqual(msg.src, "clientnode", "Source error, Excepted: clientnode, Received: " + msg.src)
#		self.assertEqual(msg.srcdock, "clientdock", "Dock error, Excepted: clientdock, Received: " + msg.srcdock)
#		self.assertEqual(msg.data, "testing", "Data error, Excepted: testing, Received: " + msg.data)
#		self.sendMsg()
		
		
if __name__ == '__main__':
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)
