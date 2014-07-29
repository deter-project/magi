#!/usr/bin/env python

import unittest2
import os
import asyncore
import logging
import sys
from magi.messaging.transport import Transport
from magi.messaging.transportTCP import TCPTransport, TCPServer
from magi.messaging.transportSSL import SSLTransport, SSLServer
from magi.messaging.transportMulticast import MulticastTransport
from magi.messaging.transportPipe import InputPipe, OutputPipe
from magi.messaging.magimessage import MAGIMessage
from magi.util.scheduler import Scheduler


class TestTCPTransport(TCPTransport):

	RXSIZE = 1  # how much data to return per call
	TXSIZE = 1  # how much data to 'send' per call

	def recv(self, maxsize):
		return Transport.recv(self, min(self.RXSIZE, maxsize))
			
	def send(self, data):
		v = data[:self.TXSIZE]
		return Transport.send(self, v)
		

class MSocket(object):
	def __init__(self, q):
		self.q = q

	def recvfrom(self, maxsize):
		pkt = self.q.pop(0)
		return (pkt, ('192.168.255.255', 12345))
			
	def sendto(self, data, addr):
		self.q.append(data)

	def close(self):
		self.q = None


class TestMulticastTransport(MulticastTransport):

	def __init__(self, inq):
		MulticastTransport.__init__(self, '239.255.1.1', 18808, '127.0.0.1')
		self.socket = MSocket(inq)



	
class TransportTest(unittest2.TestCase):
	"""
		Testing of basics in TCPTransport class
	"""

	def newMsg(self):
		msg = MAGIMessage()
		msg.msgid = 1234
		msg.flags = 0x63
		msg.contenttype = MAGIMessage.YAML
		msg.src = "mynode"
		msg.srcdock = "sourcedock"
		msg.hmac = "123456789"
		msg.dstnodes = set(['n1', 'n2'])
		msg.dstgroups = set(['g1', 'g2'])
		msg.dstdocks = set(['d1', 'd2'])
		msg.sequence = 98765
		msg.timestamp = 12347890
		msg.data = "helloworld"
		return msg

	def assertMessageEqual(self, msg1, msg2):
		for k, v in msg1.__dict__.iteritems():
			if k[0] == '_':
				continue
			self.assertEquals(getattr(msg2, k), v)


	def runAsync(self, socketmap, rx, msg, maxcount):
		bailcounter = 0
		while len(rx.inmessages) == 0:
			asyncore.poll(0.1, socketmap)
			bailcounter += 1
			if bailcounter >= maxcount:
				return False
		return True

	def test_SSL(self):
		""" Test encoding and decoding of a message through two SSL transports """
		# TCP tests already do the mass of different byte sizing for tx/rx, this is just basic SSL tests
		cafile = os.path.join(os.path.dirname(__file__), 'ca.pem')
		nodefile = os.path.join(os.path.dirname(__file__), 'node.pem')
		msg = self.newMsg()
		server = SSLServer(address='127.0.0.1', port=10102, cafile=cafile, nodefile=nodefile, matchingOU="DeterTest.bwilson-orch")
		tx = SSLTransport(address='127.0.0.1', port=10102, cafile=cafile, nodefile=nodefile, matchingOU="DeterTest.bwilson-orch")
		# match name to pem files in test
		
		# connect them, socketpair on linux doesn't work with AF_INET
		count = 0
		while True:
			asyncore.poll(0.1, {server.fileno():server, tx.fileno():tx})
			if len(server.inmessages) > 0:
				rx = server.inmessages[0]
				break
			count += 1
			self.assertLess(count, 5, 'connect failed')

		# Do a simple one time test, let TCP do the range of sizes
		mymap = { rx.fileno():rx, tx.fileno():tx }
		rx.inmessages = []
		tx.outmessages.append(msg)
		if not self.runAsync(mymap, rx, msg, 10):
			self.assert_(False, "Poll called too many times for SSL")
		self.assertMessageEqual(rx.inmessages[0], msg)

		tx.close()
		rx.close()
			

	def test_TCP(self):
		""" Test encoding and decoding of a message through two TCP transports """
		msg = self.newMsg()
		server = TCPServer('127.0.0.1', 10101)
		tx = TestTCPTransport(address='127.0.0.1', port=10101)

		# connect them, socketpair on linux doesn't work with AF_INET
		count = 0
		while True:
			asyncore.poll(0.1, {server.fileno():server, tx.fileno():tx})
			if len(server.inmessages) > 0:
				rx = server.inmessages[0]
				break
			count += 1
			self.assertLess(count, 5, 'connect failed')


		# Try different send/receive sizes to confirm there are no segment boundary issues
		mymap = { rx.fileno():rx, tx.fileno():tx }
		for ii in range(1, 100, 1):
			rx.RXSIZE = ii
			tx.TXSIZE = ii
			rx.inmessages = []
			tx.outmessages.append(msg)
			if not self.runAsync(mymap, rx, msg, 106-ii):
				self.assert_(False, "Poll called too many times for TCP with size %d" % ii)
			self.assertMessageEqual(rx.inmessages[0], msg)

		tx.close()
		rx.close()
			

	def test_Multicast(self):
		""" Test encoding and decoding of a message through two Multicast transports """
		msg = self.newMsg()
		buf = list()
		sched = Scheduler()
		tx = TestMulticastTransport(buf)
		rx = TestMulticastTransport(buf)
		tx.setScheduler(sched)
		rx.setScheduler(sched)
		tx.outmessages.append(msg)

		# Fake async for now as can't send to another on same host, gets filtered
		bailcounter = 0
		maxcount = 5
		while len(rx.inmessages) == 0:
			if tx.writable():
				tx.handle_write()
			if len(buf) > 0 and rx.readable():
				rx.handle_read()
			bailcounter += 1
			self.assert_(bailcounter < maxcount, "handle read and write called too many times %d" % maxcount)
		self.assertMessageEqual(rx.inmessages[0], msg)
		rx.inmessages = []

		# Also test a header only message
		bailcounter = 0
		maxcount = 3
		msg.contenttype = MAGIMessage.NONE
		msg.data = None
		tx.outmessages.append(msg)
		while len(rx.inmessages) == 0:
			if tx.writable():
				tx.handle_write()
			if len(buf) > 0 and rx.readable():
				rx.handle_read()
			bailcounter += 1
			self.assert_(bailcounter < maxcount, "handle read and write called too many times %d" % maxcount)
		self.assertMessageEqual(rx.inmessages[0], msg)

		tx.close()
		rx.close()


	def test_Pipe(self):
		""" Test encode and decoding of a message from an output pipe to an input pipe """
		if 'ygwin' in sys.platform and sys.version_info[1] < 5:
			raise unittest2.SkipTest("Older cygwin can't do pipes")
		try:
			os.mkfifo('/tmp/pipetest')
		except OSError:
			pass
		msg = self.newMsg()
		rx = InputPipe.fromFile('/tmp/pipetest')
		tx = OutputPipe.fromFile('/tmp/pipetest')
		tx.outmessages.append(msg)
		mymap = { rx.fileno():rx, tx.fileno():tx }
		if not self.runAsync(mymap, rx, msg, 20):
			self.assert_(False, "Poll called too many times for pipe test")
		self.assertMessageEqual(rx.inmessages[0], msg)
		tx.close()
		rx.close()


if __name__ == '__main__':
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)
