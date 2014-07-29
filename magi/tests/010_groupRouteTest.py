#!/usr/bin/env python

import unittest2
import logging
import yaml
import time
from collections import defaultdict
from magi.tests.util import TestTransport, TestMessageIntf
from magi.messaging.magimessage import MAGIMessage
from magi.messaging.routerGroup import GroupRouter, listChecksum
from magi.messaging.api import GroupRequest

class GroupRouteTest(unittest2.TestCase):
	"""
		Test basic functions of the group router.  Note, this is a straight group router,
		without any of the processing in the main loop.
	"""

	def setUp(self):
		self.transports = { 0:TestTransport(0), 1:TestTransport(1), 2:TestTransport(2) }
		self.store = TestMessageIntf()
		self.router = GroupRouter()
		self.router.configure(name="mynode", transports=self.transports, msgintf=self.store)
		self.router.transportAdded(self.transports[0])
		self.router.transportAdded(self.transports[1])
		self.router.transportAdded(self.transports[2])
		self.store.outgoing = [] # clear initial resends sent for transportAdded

	def tearDown(self):
		self.router = None

	def _checkForGroupResend(self, dst, fds):
		for msg in self.store.outgoing:
			if GroupRouter.DOCK in msg.dstdocks:
				rt = yaml.load(msg.data)
				if 'resend' in rt:
					if msg._routed == fds and dst in msg.dstnodes:
						return
					else:
						self.assert_(0, "Resend request found but incorrect fdlist/dst %s!=%s or %s!=%s" % (msg._routed, fds, msg.dstnodes, dst))
		self.assert_(0, "Resend request not found")
		
	def _checkForGroupMessage(self, rtype, groups, fds):
		for msg in self.store.outgoing:
			if GroupRouter.DOCK in msg.dstdocks:
				rt = yaml.load(msg.data)
				if rtype in rt and sorted(rt[rtype]) == sorted(groups):
					self.assert_('count' in rt, "No count value in router message %s" % rt)
					self.assert_('checksum' in rt, "No checksum value in router message %s" % rt)
					if msg._routed == fds:
						return
					else:
						self.assert_(0, "%s request found but incorrect fdlist %s!=%s" % (groups, msg._routed, fds))
		self.assert_(0, "%s request not found" % (groups))
		
	def _incomingGroupAdd(self, fd, src, grouplist):
		self.neighbors[fd][src].update(grouplist)
		request = { 'add': grouplist, 'count': len(self.neighbors[fd][src]), 'checksum': listChecksum(self.neighbors[fd][src]) }
		msg = MAGIMessage(src=src, contenttype=MAGIMessage.YAML, docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
		msg._receivedon = self.transports[fd]
		self.router.processIN([msg], time.time())

	def _incomingGroupDel(self, fd, src, grouplist):
		self.neighbors[fd][src].difference_update(grouplist)
		request = { 'del': grouplist, 'count': len(self.neighbors[fd][src]), 'checksum': listChecksum(self.neighbors[fd][src]) }
		msg = MAGIMessage(src=src, contenttype=MAGIMessage.YAML, docks=[GroupRouter.DOCK], data = yaml.safe_dump(request))
		msg._receivedon = self.transports[fd]
		self.router.processIN([msg], time.time())

	def _incomingGroupList(self, fd, src, grouplist):
		self.neighbors[fd][src] = set(grouplist)
		request = { 'set': grouplist, 'count': len(self.neighbors[fd][src]), 'checksum': listChecksum(self.neighbors[fd][src]) }
		msg = MAGIMessage(src=src, contenttype=MAGIMessage.YAML, docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
		msg._receivedon = self.transports[fd]
		self.router.processIN([msg], time.time())

	def _incomingGroupResend(self, fd, src):
		request = { 'resend': True }
		msg = MAGIMessage(src=src, contenttype=MAGIMessage.YAML, docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
		msg._receivedon = self.transports[fd]
		self.router.processIN([msg], time.time())

	def _checkGroupState(self):
		# Verify that rxGroup for any transport is Union(tx) for all other transports """
		total = len(self.transports)
		for fd in range(1, total):  # skip local rxset
			txset = set()
			for fdx in range(total):
				if fdx != fd:
					txset.update(self.router.transportGroupLists[fdx].txGroups.keys())
			rxset = set(self.router.transportGroupLists[fd].rxGroups.keys())
			self.assertEqual(rxset, txset, "rxset for %d is %s, should be %s" % (fd, rxset, txset))

		# Verify that txGroup for any transport is the same as the union of all neighbors
		for fd in range(total):
			union = set()
			for x in self.neighbors[fd].itervalues():
				union.update(x)
			self.assertEqual(sorted(self.router.transportGroupLists[fd].txGroups.keys()), sorted(union))
			

	def test_groupOperations(self):
		""" Test basic group routing operations """
		self.neighbors = {0: {}, 1: {}, 2: {}}
		for k in self.transports:
			self.neighbors[k] = defaultdict(set)

		ADD = 'add'
		DEL = 'del'
		SET = 'set'

		self._incomingGroupAdd(2, 'n1', ['g1'])
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g1'], [1])

		self._incomingGroupAdd(2, 'n1', ['g2'])
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g2'], [1])
		self.store.outgoing = []  # clear

		self._incomingGroupAdd(2, 'n1', ['g2']) # Double add, no actual change should occur
		self._checkGroupState()
		self.assertEqual(len(self.store.outgoing), 0)

		self._incomingGroupAdd(2, 'n1', ['g2', 'g3']) # Double add, only add newgroup
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g3'], [1])
		self.store.outgoing = [] # clear

		self._incomingGroupAdd(2, 'n2', ['g3', 'g4']) # Add 1 new, 1 copy from another neighbor
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g4'], [1])

		self._incomingGroupAdd(2, 'n1', ['g5', 'g6'])  # Test multiple group add
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g5', 'g6'], [1])

		self._incomingGroupAdd(1, 'n3', ['g6', 'g7'])  # Incoming from other side with overlaping groups
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g6', 'g7'], [2])
		self.store.outgoing = [] # clear
		
		self._incomingGroupDel(2, 'n1', ['g3'])  # N1 gives up g3 but n2 still holds it
		self._checkGroupState()
		self.assertEqual(len(self.store.outgoing), 0)  # no changes messages
		
		self._incomingGroupDel(2, 'n2', ['g3'])  # N2 gives up g3 , now we show a remove on 1
		self._checkGroupState()
		self._checkForGroupMessage(DEL, ['g3'], [1])
		
		self.neighbors[2]['nX'] = set(['g8', 'g9'])
		logging.disable(40) # disable error output for an expected error
		self._incomingGroupAdd(2, 'nX', ['g10'])  # A new node add but we apparently missed earlier, should send groupresend
		logging.disable(0)

		self.neighbors[2]['nX'] = set() # Fake back to what we know
		self._checkGroupState()
		self._checkForGroupResend('nX', [2])  # Should send resend back out same transport
		self.store.outgoing = [] # clear
		
		self._incomingGroupList(2, 'n1', self.neighbors[2]['n1'])  # sends its current list, should shorcut
		self._checkGroupState()
		self.assertEqual(len(self.store.outgoing), 0)  # no changes messages
		self.store.outgoing = [] # clear

		self.neighbors[2]['n1'] = set(['g11'])
		self._incomingGroupList(2, 'n1', ['g11'])  # n1 resets its list
		self._checkGroupState()
		self._checkForGroupMessage(ADD, ['g11'], [1])
		self._checkForGroupMessage(DEL, ['g1', 'g2', 'g5', 'g6'], [1])
		self.store.outgoing = [] # clear

		# neighbor on 2 asks for resend, do it
		self._incomingGroupResend(2, 'nY')
		self._checkGroupState()
		self._checkForGroupMessage(SET, self.router.transportGroupLists[2].rxGroups.keys(), [2])
		self.assertEqual(self.store.outgoing[0].dstnodes, set(['nY']))
		self.store.outgoing = [] # clear

		# Transport 2 goes down, delete remaing groups that transport 2 wanted
		self.router.transportRemoved(2, self.transports[2])
		self.neighbors[2] = {}
		save = self.transports.pop(2)
		self._checkGroupState()
		self._checkForGroupMessage(DEL, ['g4', 'g11'], [1])

		# Transport 2 comes back up, make sure we initialize rxGroups properly and send a list update
		self.transports[2] = save
		self.router.transportAdded(self.transports[2])
		self._checkGroupState()
		self._checkForGroupMessage(SET, self.router.transportGroupLists[2].rxGroups.keys(), [2])


	def test_GroupAckAggregation(self):
		""" Test aggregation of group acknowledgments """
		self.transports[3] = TestTransport(3)
		self.router.transportAdded(self.transports[3])
		self.neighbors = {0: {}, 1: {}, 2: {}, 3: {}}
		for k in self.transports:
			self.neighbors[k] = defaultdict(set)

		self._incomingGroupAdd(1, 'n11', ['g1', 'g2'])
		self._incomingGroupAdd(1, 'n12', ['g3'])

		self._incomingGroupAdd(2, 'n21', ['g1', 'g2'])
		self._incomingGroupAdd(2, 'n22', ['g1', 'g3'])
		self._checkGroupState()

		# Check the processing of a message with groups that wants an ack, verify data structure inside
		msg = MAGIMessage(src="n30", nodes=["n21"], groups=['g1', 'g3'])
		msg.msgid = 789
		msg.flags |= MAGIMessage.WANTACK
		msg._receivedon = self.transports[3]
		msg._routed = self.router.routeMessage(msg)

		self.assertEqual(msg._routed, set([1, 2]))
		out = self.router.processFWD([msg], time.time())
		self.assertEquals(out, [msg])

		key = ("n30", 789)
		self.assertEquals(self.router.ackHolds.keys(), [key])
		self.assertEquals(sorted(self.router.ackHolds[key].keys()),	['g1', 'g3'])
		self.assertEquals(self.router.ackHolds[key]['g1'][1],		set(['n11']))
		self.assertEquals(self.router.ackHolds[key]['g1'][2],		set(['n21', 'n22']))
		self.assertEquals(self.router.ackHolds[key]['g3'][1],		set(['n12']))
		self.assertEquals(self.router.ackHolds[key]['g3'][2],		set(['n22']))
		self.store.outgoing = []  # clear
		
		# from N11, ack stops here
		ack = msg.createAck("", ['g1']) 
		ack._receivedon = self.transports[1]
		ack.src = "n11"
		out = self.router.processFWD([ack], time.time())  # no group routing, only node routing
		self.assertEquals(self.router.ackHolds[key]['g1'].keys(),	[2]) # nothing left on transport 1 for g1
		self.assertEquals(0, len(out))

		# from N21, ack passes with n21 but no groups
		ack = msg.createAck("n21", ['g1']) 
		ack.src = "n21"
		ack._receivedon = self.transports[2]
		out = self.router.processFWD([ack], time.time())
		self.assertEquals(self.router.ackHolds[key]['g1'][2],		set(['n22']))
		self.assertEquals(1, len(out))
		self.assertEquals("n21", out[0].data)

		# from N22, ack passes with g1
		ack = msg.createAck("", ['g1', 'g3']) 
		ack.src = "n22"
		ack._receivedon = self.transports[2]
		out = self.router.processFWD([ack], time.time())
		self.assertEquals(self.router.ackHolds[key].keys(), ['g3'])  # g1 complete
		self.assertEquals(self.router.ackHolds[key]['g3'].keys(), [1]) # g3 is still active on transport 1
		self.assertEquals(1, len(out))
		self.assertEquals(",g1", out[0].data)

		# from N12, ack passes with g2
		ack = msg.createAck("", ['g3']) 
		ack.src = "n12"
		ack._receivedon = self.transports[1]
		out = self.router.processFWD([ack], time.time())
		self.assertEquals(self.router.ackHolds.keys(), [])  # all complete
		self.assertEquals(1, len(out))
		self.assertEquals(",g3", out[0].data)

	def test_Checksum(self):
		""" Verify that checksum is as expected on all platforms and encoding """
		self.assertEqual(listChecksum(["group1", "http2", "web3"]), 809305538)
		self.assertEqual(listChecksum(["group1", "http2", "web3", "ftp4", "ssh5", "Harpoon6"]), 3224243152)

	def test_routeAll(self):
		""" Test route with a group ALL """
		msg = MAGIMessage(groups=[GroupRouter.ALLNODES])
		msg._receivedon = self.transports[1]
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([0,1,2]))

	def test_routeNeighLocal(self):
		""" Test external message sent to NEIGH """
		msg = MAGIMessage(groups=[GroupRouter.ONEHOPNODES])
		msg._receivedon = self.transports[1]
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([0]))
		
	def test_routeNeighExt(self):
		""" Test internal message sent to NEIGH """
		msg = MAGIMessage(groups=[GroupRouter.ONEHOPNODES])
		msg._receivedon = self.transports[0]
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([1,2]))
		
	def test_routeNone(self):
		""" Test message without any groups """
		msg = MAGIMessage(nodes=["somenode"])
		msg._receivedon = self.transports[1]
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([]))

	def test_remoteGroup(self):
		""" Test message with a remote group and transport down """
		addother = { 'add': ['othergroup'], 'count': 1, 'checksum': listChecksum(['othergroup']) }
		rtmsg1 = MAGIMessage(groups=[GroupRouter.ONEHOPNODES], contenttype=MAGIMessage.YAML, docks=[GroupRouter.DOCK], data=yaml.safe_dump(addother))
		rtmsg1._receivedon = self.transports[2]

		adddead = { 'add': ['deadgroup'], 'count': 2, 'checksum': listChecksum(['deadgroup', 'othergroup']) }
		rtmsg2 = MAGIMessage(groups=[GroupRouter.ONEHOPNODES], contenttype=MAGIMessage.YAML, docks=[GroupRouter.DOCK], data=yaml.safe_dump(adddead))
		rtmsg2._receivedon = self.transports[2]

		msg = MAGIMessage(groups=["othergroup"])
		msg._receivedon = self.transports[1]
 
		# Local joins mygroup, on transport2 they join othergroup and deadgroup, message enters on transport1 for othergroup
		self.router.groupRequest(GroupRequest("join", "mygroup", "default"))
		self.router.processIN([rtmsg1], time.time())
		self.router.processIN([rtmsg2], time.time())
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([2]))

		# Now transport is flagged as down, same message should route to nowhere
		self.router.transportRemoved(2, self.transports[2])
		msg._appendedto = set()
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set())

	def test_localGroup(self):
		""" Test message with a local group """
		msg = MAGIMessage(groups=["mygroup"])
		msg._receivedon = self.transports[1]
 
		# Local joins mygroup, message enters on transport1 for mygroup
		self.router.groupRequest(GroupRequest("join", "mygroup", "default"))
		fds = self.router.routeMessage(msg)
		self.assertEqual(fds, set([0]))

	def test_joinFlags(self):
		""" Make sure flags assigned to group joins and leaves keep proper groups active """
		# no prsent
		self.assertFalse("mygroup" in self.router.transportGroupLists[0].txGroups)
		# Present
		self.router.groupRequest(GroupRequest("join", "mygroup", "obj1"))
		self.assertTrue("mygroup" in self.router.transportGroupLists[0].txGroups)
		# Present
		self.router.groupRequest(GroupRequest("join", "mygroup", "obj2"))
		self.assertTrue("mygroup" in self.router.transportGroupLists[0].txGroups)
		# Present
		self.router.groupRequest(GroupRequest("leave", "mygroup", "obj1"))
		self.assertTrue("mygroup" in self.router.transportGroupLists[0].txGroups)
		# No longer present
		self.router.groupRequest(GroupRequest("leave", "mygroup", "obj2"))
		self.assertFalse("mygroup" in self.router.transportGroupLists[0].txGroups)
		

if __name__ == '__main__':
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.INFO)
	unittest2.main(verbosity=2)

