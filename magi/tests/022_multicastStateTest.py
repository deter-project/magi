#!/usr/bin/env python

import unittest2
import logging
import time
from magi.messaging.magimessage import MAGIMessage, DefaultCodec
from magi.messaging.transportMulticast import MulticastTransport, MCTHeader, TXMessageTracker
from magi.util.scheduler import Scheduler


class TransportTest(unittest2.TestCase):
	"""
		Testing of basics in TCPTransport class
	"""

	def setUp(self):
		self.sched = Scheduler()
		self.mct = MulticastTransport('239.255.1.1', 18808, '127.0.0.1')
		self.mct.setScheduler(self.sched)
		self.msgid = 1234
		self.msgcodec = DefaultCodec()


	def countMessages(self, mtype):
		count = 0
		for msg in self.mct.outmessages:
			if msg.type == mtype:
				count += 1
		return count


	def addBigMessage(self, mid, count, skip):
		msg = MAGIMessage()
		msg.msgid = 12345
		msg.contenttype = MAGIMessage.NONE
		msg.src = "mynode"
		msg.srcdock = "sourcedock"
		msg.dstgroups = ['g1']
		msg.data = "X" * (((TXMessageTracker.SPLITSIZE-4) * count) - len(self.msgcodec.encode(msg)))
		tracker = TXMessageTracker(msg=msg, multicastid=mid, codec=DefaultCodec())
		for ii in range(1,count+1):
			if ii not in skip:
				self.mct.processPacket('192.168.1.1', 18808, tracker.getPart(ii))

	def addMsg(self, msgid):
		self.msgid += 1
		msg = MAGIMessage()
		msg.msgid = self.msgid
		msg.contenttype = MAGIMessage.NONE
		msg.src = "mynode"
		msg.srcdock = "sourcedock"
		msg.dstgroups = ['g1']
		msg.data = None
		self.mct.processPacket('192.168.1.1', 18808, MCTHeader.PktData(msgid, 1, 1).encode() + self.msgcodec.encode(msg))

	def addStatus(self, msgid, boottime):
		self.mct.processPacket('192.168.1.1', 18808, MCTHeader.PktStat(msgid, boottime).encode()) 

	def addReq(self, src, msgid, pieces):
		self.mct.processPacket('192.168.1.1', 18808, MCTHeader.PktReq(msgid, src, *pieces).encode()) 

	def runEncoding(self):
		""" Runs the encoding process on each tx message to make sure they encode correctly """
		for m in self.mct.outmessages:
			m.encode()

	def test_BasicRequest(self):
		""" Test of basic restransmit request based on status messages """
		self.addStatus(2, 123)
		self.addStatus(5, 123)
		self.sched.run()
		self.assertEqual(0, self.countMessages(MCTHeader.PKTREQ))
		self.addStatus(7, 123)
		self.sched._doall()
		self.assertEqual(5, self.countMessages(MCTHeader.PKTREQ))  # requests for 3, 4, 5, 6, 7
		self.assertEqual(self.mct.outmessages[-2].type, MCTHeader.PKTREQ) # Correct type
		self.assertEqual(self.mct.outmessages[-2].src, '192.168.1.1')  # Correct source
		self.assertEqual(self.mct.outmessages[-2].pieces, (0,))  # Correct parts
		self.runEncoding()
		
	def test_RequestPlusMessages(self):
		""" Test of restransmit request when receiving messages before request is made """
		self.addStatus(2, 123)
		self.addStatus(7, 123)
		self.sched.run()
		self.assertEqual(0, self.countMessages(MCTHeader.PKTREQ))
		self.addMsg(3)
		self.addMsg(4)
		self.sched._doall()
		self.assertEqual(3, self.countMessages(MCTHeader.PKTREQ))  # requests for 5, 6, 7
		self.assertEqual(self.mct.outmessages[-2].type, MCTHeader.PKTREQ) # Correct type
		self.assertEqual(self.mct.outmessages[-2].src, '192.168.1.1')  # Correct source
		self.assertEqual(self.mct.outmessages[-2].pieces, (0,))  # Correct parts
		self.runEncoding()

	def test_DeadResponse(self):
		""" Test receipt of request and return of dead packet response """
		self.addReq('127.0.0.1', 5, (0,))
		self.assertEqual(len(self.mct.outmessages), 2)  # status is always inserted when we start
		self.assertEqual(self.mct.outmessages[0].type, MCTHeader.PKTDEAD) # Dead Message is inserted at beginning of queue
		self.assertEqual(self.mct.outmessages[0].multicastid, 5)
		self.runEncoding()

	def test_NackSupression(self):
		""" Test supression of requests when another request(NACK) is seen """
		self.addStatus(2, 123)  # sets finished to 1
		self.addStatus(7, 123)
		self.addReq('192.168.1.1', 5, (0,))
		self.addReq('192.168.1.1', 6, (0,))
		self.addReq('192.168.1.1', 7, (0,))
		self.sched._doall()
		self.assertEqual(2, self.countMessages(MCTHeader.PKTREQ))  # requests for 3,4
		self.runEncoding()

	def test_PartialNackSupression(self):
		""" Test NACK supression when multiple parts to a message """
		self.addStatus(2, 123)  # sets finished to 1
		self.addBigMessage(2, 10, [3,7,8])  # message 2 delivered missing parts 3, 7 and 8
		self.addReq('192.168.1.1', 2, (1, 7, 8))  # Someone else requests 7 and 8 as well in addition to 1 which we already have
		self.sched._doall()
		self.assertEqual(0, self.countMessages(MCTHeader.PKTREQ))  # no requests
		self.runEncoding()


if __name__ == '__main__':
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)
