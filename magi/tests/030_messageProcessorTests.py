#!/usr/bin/env python

import unittest2
from magi.tests.util import TestTransport, TestMessageIntf
from magi.util.scheduler import Scheduler
from magi.messaging.processor import *

class MessageProcessorTest(unittest2.TestCase):
	"""
		Testing of message processors
	"""

	def setUp(self):
		self.transports = { 0:TestTransport(0), 1:TestTransport(1), 2:TestTransport(2) }
		self.store = TestMessageIntf()

	def test_AckReply(self):
		""" Test ACK replier """
		proc = AckReply()
		proc.configure(name="myname", msgintf=self.store, transports=self.transports)
		
		# Regular message passes without action
		msg = MAGIMessage(nodes=["myname"])
		ret = proc.processIN([msg], time.time())
		self.assertEquals([msg], ret)

		# Ack message passes without action
		msg = MAGIMessage(nodes=["myname"])
		msg.msgid = 999
		msg.flags |= MAGIMessage.ISACK
		msg._receivedon = self.transports[1]

		ret = proc.processIN([msg], time.time())
		self.assertEquals([msg], ret)
		self.assertEquals(len(self.store.outgoing), 0)

		# WantAck message passes but causes an ack to be sent
		msg = MAGIMessage(src="me", nodes=["myname"])
		msg.msgid = 42
		msg.flags |= MAGIMessage.WANTACK
		msg._receivedon = self.transports[1]

		ret = proc.processIN([msg], time.time())
		self.assertEquals([msg], ret)
		self.assertEquals(len(self.store.outgoing), 1)
		self.assertEquals(self.store.outgoing[0]._routed, [1])
		self.assertEquals(self.store.outgoing[0].data, "42,myname")  # ack to myname but no group data


		
	def test_NamedAndID(self):
		""" Test Name and ID message processor """ 
		proc = NameAndID()
		proc.configure(name="myname", msgintf=self.store, transports=self.transports)
		
		# Name should always become myname, msgid should be incrementing
		msg = MAGIMessage()
		proc.processOUT([msg], time.time())
		self.assertEquals(msg.src,  "myname")
		firstid = msg.msgid

		msg = MAGIMessage()
		proc.processOUT([msg], time.time())
		self.assertEquals(msg.src,  "myname")
		self.assertEquals(msg.msgid, firstid+1)

		msg = MAGIMessage()
		proc.processOUT([msg], time.time())
		self.assertEquals(msg.src,  "myname")
		self.assertEquals(msg.msgid, firstid+2)

		# First message should pass, second should get dropped as a duplicate
		ret = proc.processIN([msg], time.time())
		self.assertEquals([msg], ret)
		
		ret = proc.processIN([msg], time.time())
		self.assertEquals([], ret)

		# Fill idlist buffer, force it to clean and make sure it still works
		for ii in range(1, 300):
			msg = MAGIMessage(src="n1")
			msg.msgid = ii
			ret = proc.processIN([msg], time.time())
			self.assertEquals([msg], ret)
			self.assert_(len(proc.lists["n1"]) < 200, "single id list should not reach 200")


	def test_AckProcessor(self):
		""" Test ACK processor """
		proc = AckRequirement()
		proc.configure(name="myname", msgintf=self.store, transports=self.transports, scheduler=Scheduler())

		# No flags, should always have flag cleared
		msg = MAGIMessage()
		msg.flags |= MAGIMessage.WANTACK
		proc.processOUT([msg], time.time())
		self.assert_(not msg.wantsAck())
		self.assertEquals(len(proc.inflight), 0)

		# When asking for ACK, flag should be set and message saved
		taggedmsg = MAGIMessage(nodes="n1,n2,n3", groups="g1,g2,g3", data="acktest")
		taggedmsg.msgid = 7
		taggedmsg._userargs = {'acknowledgement':True }
		proc.processOUT([taggedmsg], time.time())
		self.assert_(taggedmsg.wantsAck())
		self.assertEquals(len(proc.inflight), 1)

		# An ack comes back for n1 and g1, should be removed
		ackmsg = MAGIMessage(data="7,n1,g1")
		ackmsg.msgid = 999
		ackmsg.flags |= MAGIMessage.ISACK
		proc.processIN([ackmsg], time.time())
		self.assertEquals(proc.inflight[7].msg.dstnodes, set(['n2', 'n3']))
		self.assertEquals(proc.inflight[7].msg.dstgroups, set(['g2', 'g3']))

		# Another ack just for g2, g2 will be removed
		ackmsg = MAGIMessage(data="7,,g2")
		ackmsg.msgid = 888
		ackmsg.flags |= MAGIMessage.ISACK
		proc.processIN([ackmsg], time.time())
		self.assertEquals(proc.inflight[7].msg.dstnodes, set(['n2', 'n3']))
		self.assertEquals(proc.inflight[7].msg.dstgroups, set(['g3']))

		# Another ack just for n2 
		ackmsg = MAGIMessage(data="7,n2,")
		ackmsg.msgid = 777
		ackmsg.flags |= MAGIMessage.ISACK
		proc.processIN([ackmsg], time.time())
		self.assertEquals(proc.inflight[7].msg.dstnodes, set(['n3']))
		self.assertEquals(proc.inflight[7].msg.dstgroups, set(['g3']))

		# A second n2, nothing changes
		proc.processIN([ackmsg], time.time())
		self.assertEquals(proc.inflight[7].msg.dstnodes, set(['n3']))
		self.assertEquals(proc.inflight[7].msg.dstgroups, set(['g3']))

		# Doesn't absorb normal messages
		msg = MAGIMessage()
		ret = proc.processIN([msg], time.time())
		self.assertEquals([msg], ret)
		self.assertEquals(len(proc.inflight), 1)

		start = time.time()
		while True:
			time.sleep(0.1)
			total = time.time() - start
			proc.scheduler.run()
			if total < 0.4:
				self.assertEquals(0, len(self.store.outgoing))
			elif total < 0.6:
				continue  # add some fuzz for 0.4-0.6
			elif total < 1.4:
				self.assertEquals(1, len(self.store.outgoing))
			elif total < 1.6:
				continue  # add some fuzz for 1.4-1.6
			elif total < 3.4:
				self.assertEquals(2, len(self.store.outgoing))
			elif total < 3.6:
				continue  # add some fuzz for 3.4-3.6
			elif total < 7.4:
				self.assertEquals(3, len(self.store.outgoing))
			elif total < 7.6:
				continue # add some fuzz for 7.4-7.6
			elif total < 15.4:
				self.assertEquals(4, len(self.store.outgoing))
				continue
			elif total < 15.6:
				continue # add some fuzz for 15.4-15.6
			elif total > 15.6:
				self.assertEquals(len(self.store.outgoing), 4) # no more acks
				self.assertEquals(len(proc.inflight), 0) # old messages is gone
				self.assertEquals(len(self.store.status), 1) # error message incoming
				self.assertEquals(self.store.status[0].status, "Dropping packet after too many restransmits")
				self.assertEquals(self.store.status[0].isack, False)
				self.assertEquals(self.store.status[0].msg, taggedmsg)
				break
		

	def test_SequenceProcessor(self):
		""" Test Sequence Processor """
		proc = SequenceRequirement()
		proc.configure(name="myname", msgintf=self.store, transports=self.transports)

		# No flags, should clear any sequence set
		msg = MAGIMessage()
		msg.sequence = 42
		proc.processOUT([msg], time.time())
		self.assertEquals(None, msg.sequence)

		# When asking for source ordering, sequence should be added and incremented
		msg = MAGIMessage(nodes="n1")
		msg._userargs = {'source_ordering':555 }
		proc.processOUT([msg], time.time())
		self.assertEquals(1, msg.sequence)
		self.assertEquals(555, msg.sequenceid)
		proc.processOUT([msg], time.time())
		self.assertEquals(2, msg.sequence)
		self.assertEquals(555, msg.sequenceid)

		# if the ID changes, we should get a different counter
		msg._userargs = {'source_ordering':556 }
		ret = proc.processOUT([msg], time.time())
		self.assertEquals(ret, [msg])
		self.assertEquals(1, msg.sequence)
		self.assertEquals(556, msg.sequenceid)

		# if the ID is the same but the dest list change, it should cause and error
		msg.dstgroups = ['g1']
		ret = proc.processOUT([msg], time.time())
		self.assertEquals([], ret)
		self.assertEquals(len(self.store.status), 1) # error message incoming
		self.assertEquals(self.store.status[0].status, "Destination parameters cannot change in a sequence")
		self.assertEquals(self.store.status[0].isack, False)

		# Doesn't absorb normal messages
		msg = MAGIMessage()
		ret = proc.processIN([msg], time.time())
		self.assertEquals([msg], ret)

		# seqence init to 2 and returned, 1 is dropped, 4,3 get ordered into 3,4
		msg1 = MAGIMessage(src="n1", groups=['g1'], sequence=1, sequenceid=555)
		msg2 = MAGIMessage(src="n1", groups=['g1'], sequence=2, sequenceid=555)
		msg3 = MAGIMessage(src="n1", groups=['g1'], sequence=3, sequenceid=555)
		msg4 = MAGIMessage(src="n1", groups=['g1'], sequence=4, sequenceid=555)

		msg21 = MAGIMessage(src="n1", groups=['g1', 'g2'], sequence=1, sequenceid=556)
		msg22 = MAGIMessage(src="n1", groups=['g2', 'g1'], sequence=2, sequenceid=556)
		msg23 = MAGIMessage(src="n1", groups=['g2', 'g1'], sequence=3, sequenceid=556)

		ret = proc.processIN([msg2], time.time())
		self.assertEquals([msg2], ret)

		ret = proc.processIN([msg1], time.time())  # dropped as sequence initiated at 2
		self.assertEquals([], ret)

		ret = proc.processIN([msg22], time.time())  # initiates new sequence at 2 as well
		self.assertEquals([msg22], ret)

		ret = proc.processIN([msg21], time.time())  # another drop due to previous init
		self.assertEquals([], ret)

		ret = proc.processIN([msg4], time.time())
		self.assertEquals([], ret)

		ret = proc.processIN([msg23], time.time())
		self.assertEquals([msg23], ret)

		ret = proc.processIN([msg3], time.time())
		self.assertEquals([msg3,msg4], ret)


	def test_TimestampProcessor(self):
		""" Test Timestamp Processor """
		proc = TimestampRequirement()
		proc.configure(name="myname", msgintf=self.store, transports=self.transports)

		# No flags, should clean any timestamp set
		msg = MAGIMessage()
		msg.timestamp = 42
		proc.processOUT([msg], time.time())
		self.assertEquals(None, msg.timestamp)

		# When asking for timestamping, timestamp should be added 
		msg = MAGIMessage()
		msg._userargs = { 'timestamp':1234567 }
		proc.processOUT([msg], time.time())
		self.assertEquals(1234567, msg.timestamp)

		# Regular message passes as does time in past, time in future is held
		now = time.time()
		now1 = now+1
		msg1 = MAGIMessage()
		msg2 = MAGIMessage(timestamp=now-1)
		msg3 = MAGIMessage(timestamp=now1)

		ret = proc.processIN([msg1], time.time())
		self.assertEquals([msg1], ret)

		ret = proc.processIN([msg3], time.time())
		self.assertEquals([], ret)
		self.assertEquals(1, len(proc.heap))

		ret = proc.processIN([msg2], time.time())
		self.assertEquals([msg2], ret)
		self.assertEquals(len(self.store.pushmap), 1)
		self.assertEquals(self.store.pushmap['IN'], now1)

		time.sleep(1.001)
		ret = proc.processIN([], time.time())
		self.assertEquals([msg3], ret) #self.store.incoming)


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

