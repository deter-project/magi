"""

Group routing spreads its information in a fashion similar to IGMP.  It sends group information
in ADD group, DEL group and full list messages.  The messages contain the full list count and a
CRC to make sure everyone is in sync.

Internally, each transport maintains a list of groups that it wishes to receive and which become
the group list that it will request from its neighbors.  We call this the rx list.

Each transport also maintains a list of groups that have been requested by other neighbors and it
used by the router code to route messages.  We call this the tx list.

If there are three transports, T1, T2, T3, then:

T1.txlist = union(neighbor requested lists)  - this is controlled by neighbors on the interface
T1.rxlist = union(T2.txlist, T3.txlist)

Each transport also maintains separate lists for each neighbor for state matching and to help
determine when a DEL request indicates that no neighbors request the group any longer and it 
can be removed the the transport txlist.  This is only necessary for multicast transport or
those with multiple neighbors.  Point-Point transports should have a short-cut version.

To aid in the multiple neighbor requests, the transport txlist is actually a counting set that
records the number of adds/deletes so that each new messages can be processed quickly without 
having to do unions of a large number of sets each time (really just for delete operations).

The counting set will return True when an inc operation creates a new entry and a dec operation
removes an entry (when the value goes to 0).  This indicates a change in the union of of the neighbor
lists.  Only when a change oocurs will notifications be made to the other transports.

Each transport will maintain another counting set for groups that it is requesting from its neighbors.
In this case, the counts represent the other transports that are requesting the group.  Once that number
falls to zero, it no longer requests it and when its created, a new request is made.


"""

import logging
import zlib
import yaml
from collections import defaultdict
from magi.messaging.api import MAGIMessage
from magi.messaging.processor import BlankRouter
from magi.util.Collection import CountingSet

log = logging.getLogger(__name__)


class GroupStateError(Exception):
	pass

def listChecksum(grouplist):
	""" Calculate the checksum used in router messages """
	checksum = 1  # same as Adler32 in java zlib
	for group in sorted(grouplist):
		checksum = zlib.adler32(group, checksum) 
	return checksum & 0xffffffff



class NeighborGroupList(object):
	"""
		Storage of current state information for a single node on a transport directly connected to us
		nodeGroups - the groups this neighbor requested
		checksum - the calculated checksum of the group list in sorted() order
	"""
	__slots__ = ['nodeGroups', 'checksum']
	def __init__(self):
		self.nodeGroups = set()
		self.checksum = 0

	def _verify(self, count, checksum):
		if len(self.nodeGroups) != count:
			raise GroupStateError("Invalid group count, local %d vs remote %d" % (len(self.nodeGroups), count))
		localchecksum = listChecksum(self.nodeGroups)
		if localchecksum != checksum:
			raise GroupStateError("Invalid checksum, local %d vs remote %d" % (localchecksum, checksum))

	def add(self, count, checksum, groupset):
		ret = groupset - self.nodeGroups  # return only what gets added, ignore doubles
		self.nodeGroups |= ret
		self._verify(count, checksum)
		return ret

	def remove(self, count, checksum, groupset):
		ret = groupset & self.nodeGroups # return only what gets removed, i.e. we don't remove what isn't already there
		self.nodeGroups -= ret
		self._verify(count, checksum)
		return ret

	def newlist(self, count, checksum, groupset):
		self.nodeGroups = groupset
		try:
			self._verify(count, checksum)
		except GroupStateError, e:
			# this is really bad if the checksum fails here, it means we are not calculating correctly
			log.severe("Recevied a new list but checks failed.  Stopping loop here, groups will be out of sync: %s", e)
			return




class TransportGroupList(object):
	"""
		Storage for group information on a single transport.
		msgintf - link to the msgintf for the group router processor, for sending new group messages
		srccache - a mapping from src node to NeighborGroupList, needed for removal on shared transports like multicast
		txGroups - the list of actual groups to send out this transport based on info from neighbors
		rxGroups - the list that is sent out this transport to inform others of our list of groups we want to route/receive
	"""
	
	__slots__ = ['msgintf', 'fileno', 'srccache', 'txGroups', 'rxGroups']
	def __init__(self, msgintf, fileno):
		self.msgintf = msgintf
		self.fileno = fileno
		self.srccache = dict()
		self.txGroups = CountingSet() 
		self.rxGroups = CountingSet()

	def join(self, group):
		return self.txGroups.inc(group)
			
	def leave(self, group):
		return self.txGroups.dec(group)
			
	def processMessage(self, src, request):
		"""
			Processes incoming routing messages, determine what has changed in our state and return that.  An error response may be
			sent to the sender if a synchronization problem is detected.
		"""
		# Update groups for this src, recreate the full set
		if src not in self.srccache:
			self.srccache[src] = NeighborGroupList()
		nentry = self.srccache[src]

		tadded = list()
		tremoved = list()

		try:
			if 'add' in request:
				added = nentry.add(request['count'], request['checksum'], set(request['add']))
				tadded = self.txGroups.incGroup(added)
	
			if 'del' in request:
				removed = nentry.remove(request['count'], request['checksum'], set(request['del']))
				tremoved = self.txGroups.decGroup(removed)
				
			if 'set' in request:
				if 'add' in request or 'del' in request:
					log.error("Got a group route message with a bad set of requests (%s)", request.keys())
				else:
					if request['count'] == len(nentry.nodeGroups) and request['checksum'] == nentry.checksum: 
						return ([], []) # Nothing actually changed according to our list, shortcut it here
	
					namelist = set(request['set'])
					for value in nentry.nodeGroups - namelist:  # All entries that are no longer in the new list
						if self.txGroups.dec(value):
							tremoved.append(value)
					for value in namelist - nentry.nodeGroups: # All entries that are new to the list
						if self.txGroups.inc(value):
							tadded.append(value)
		
					# Actually apply the change to the neighbor entry
					nentry.newlist(request['count'], request['checksum'], namelist)

		except GroupStateError:
			msg = MAGIMessage(contenttype=MAGIMessage.YAML, nodes=[src], docks=[GroupRouter.DOCK], data=yaml.safe_dump({'resend':True}))
			msg._routed = [self.fileno]
			self.msgintf.send(msg)
			raise

		# Return the lists that we changed
		return (tadded, tremoved)


	def requestChanges(self, added, deleted):
		"""
			Caller is letting us know that another transport has changed the list of groups it wishes to know about.
			This method will determine what message it needs to send (if any) out the associated transport
		"""
		if self.fileno == 0:
			return # shortcut for localhost

		# Determine what addition message to send, if any
		request = dict()
		additions = list()
		for g in added:
			if self.rxGroups.inc(g):
				additions.append(g)  # This is actually new to us
		if len(additions) > 0:
			request['add'] = additions

		# Determine what removal message to send, if any
		deletions = list()
		for g in deleted:
			if self.rxGroups.dec(g):
				deletions.append(g)  # Noone appears to want this anymore
		if len(deletions) > 0:
			request['del'] = deletions
		
		# If something did change send out the updates
		if len(additions) > 0 or len(deletions) > 0:
			request['count'] = len(self.rxGroups)
			request['checksum'] = listChecksum(self.rxGroups)
			msg = MAGIMessage(contenttype=MAGIMessage.YAML, groups=[GroupRouter.ONEHOPNODES], docks=[GroupRouter.DOCK], data=yaml.safe_dump(request))
			msg._routed = [self.fileno]
			self.msgintf.send(msg)



class GroupRouter(BlankRouter):
	"""
		Maintains list of groups as announced on each transport in addition to the local node.
		Mapped by fileno, 0 is used for local node.
		When required a GroupAnnouncement is sent out applicable transports.
		The group list announced is the set of groups for all transports not including the one being transmitted on.
		i.e. we announce for ourselves as well as other transports.
	"""

	ALLNODES = "__ALL__"
	ONEHOPNODES = "__NEIGH__"
	DOCK = "__GROUPS__"

	def __init__(self):
		BlankRouter.__init__(self)
		self.transportGroupLists = dict()
		self.ackHolds = dict() # storage for group ack aggregation
		self.localGroupFlags = defaultdict(set)  # used to filter local join/leave requests when multiple callers are involved


	def configure(self, name="missing", scheduler=None, msgintf=None, transports=None, stats=None, **kwargs):
		""" Can't finish our initialization until we get a msgintf pointer """
		BlankRouter.configure(self, name, scheduler, msgintf, transports, stats, **kwargs)
		self.transportGroupLists[0] = TransportGroupList(self.msgintf, 0)


	def groupRequest(self, req):
		"""
			Received a group request from the user process.  We store this group info under a special
			TransportGroup addressed by fd==0.  The request consists of a type (join, leave), the group name,
			and a string value that indicates the process/thread/object that requested the join
		"""
		log.debug("received request %s", req)
		if req.type == 'join':
			# Filter out repeated calls with same group,caller when already active
			if req.caller in self.localGroupFlags[req.group]: return 
			self.localGroupFlags[req.group].add(req.caller)

			# TransportGroupList takes care of calls from different callers 
			if self.transportGroupLists[0].join(req.group):
				# the join actually changed state, update info and send message
				for fd, tgl in self.transportGroupLists.iteritems():
					if fd == 0: continue
					tgl.requestChanges([req.group], [])

		elif req.type == 'leave':
			# Filter out repeated calls with same group,caller when not already active
			if req.caller not in self.localGroupFlags[req.group]: return
			self.localGroupFlags[req.group].discard(req.caller)

			# TransportGroupList takes care of calls from different callers 
			if self.transportGroupLists[0].leave(req.group):
				# the leave actually changed state, update info and send message
				for fd, tgl in self.transportGroupLists.iteritems():
					if fd == 0: continue
					tgl.requestChanges([], [req.group])


	def processIN(self, msglist, now):
		"""
			Received a group announcement, process and see if we need to send any updates ourselves.
			Returns true we need to send updates out other transports. False for no change made.

		"""
		passed = list()
		for msg in msglist:
			if GroupRouter.DOCK in msg.dstdocks:
				self.groupRouterMessage(msg)
			else:
				passed.append(msg)

		return passed


	def groupRouterMessage(self, msg):
		try:
			tgl = self.transportGroupLists[msg._receivedon.fileno()]
			request = yaml.load(msg.data)
			response = None

			if 'resend' in request:
				response = {
					'set': list(tgl.rxGroups),
					'count': len(tgl.rxGroups),
					'checksum': listChecksum(tgl.rxGroups)
				}
				listmsg = MAGIMessage(contenttype=MAGIMessage.YAML, nodes=[msg.src], docks=[GroupRouter.DOCK], data=yaml.safe_dump(response))
				listmsg._routed = [msg._receivedon.fileno()]
				self.msgintf.send(listmsg)

			else:
				# Discover what actually changed
				(added, deleted) = tgl.processMessage(msg.src, request)
				# Pass changes onto other transport for processing
				for fd, othertgl in self.transportGroupLists.iteritems():
					if othertgl is tgl: continue
					othertgl.requestChanges(added, deleted)

		except Exception, e:
			log.error("Failed to process router message: %s", e, exc_info=1)


	def processFWD(self, msglist, now):
		"""
			Dealing with group ack aggregation
		"""
		for msg in msglist:
			if msg.isAck():
				self.processGroupAckReply(msg)
				if msg.data == "":  # No more ack data left, don't bother sending
					msglist.remove(msg)
			elif msg.wantsAck():
				self.processGroupAckRequest(msg)
		return msglist


	def processGroupAckRequest(self, msg):
		"""
			For messages passing through this node, if they are requesting an ACK, we make note of groups 
			in the message and neighbors that are a part of those groups for later aggregation. 
			Storage looks like:
				{ (msg.src,msg.id):
						groupname:
							transportid:
								set([neighbors we require an ack from])
				}
		"""
		if (msg.src, msg.msgid) in  self.ackHolds:
			return  # We only process the first one
		log.debug("Make note of group to ack (%s, %s)", msg.src, msg.msgid)
		tracker = dict()
		self.ackHolds[(msg.src, msg.msgid)] = tracker
		for group in msg.dstgroups:
			tracker[group] = dict()
			for fd in msg._routed:
				tracker[group][fd] = set()
				tgl = self.transportGroupLists[fd]
				for node, nlist in tgl.srccache.iteritems():  # node is a neighbor, nlist is its NeighborGroupList
					if group in nlist.nodeGroups:
						log.debug("Marking neighbor in groupack box: %s,%s -> %s", group, fd, node)
						tracker[group][fd].add(node)


	def processGroupAckReply(self, msg):
		"""
			For acks returning, we remove the neighbor from the list.  If all of the necessary neighbors have replied
			than we pass it on the ack with the group info, otherwise we strip it.
		"""
		ackdata = msg.data.split(',')
		if len(ackdata) <= 2:
			return

		ackid = int(ackdata[0])
		key = (list(msg.dstnodes)[0], ackid)
		if key not in self.ackHolds:
			return
		tracker = self.ackHolds[key]
		rxfd = msg._receivedon.fileno()

		ackgroups = list()
		for group in ackdata[2:]:
			if group not in tracker: continue  # shortcuts
			if rxfd  not in tracker[group]: continue
			log.debug("Group ack in: %s,%s <- %s", group, rxfd, msg.src)
			tracker[group][rxfd].discard(msg.src)  # check this guy off the list
			if len(tracker[group][rxfd]) == 0:  # trim this branch
				del tracker[group][rxfd]
			if len(tracker[group]) == 0:  # trim up the trunk, also note the complete group
				ackgroups.append(group)
				del tracker[group]
			if len(tracker) == 0:  # whole message is done
				log.debug("Group acks complete for %s", key)
				del self.ackHolds[key]

		msg.data = ','.join(ackdata[1:2] + ackgroups)




	def routeMessage(self, msg):
		""" Return a list of all the transport filenos this message should be sent out based on group names """
		if GroupRouter.ALLNODES in msg.dstgroups:
			return set(self.transports.keys())

		if GroupRouter.ONEHOPNODES in msg.dstgroups:
			if msg._receivedon.fileno() == 0:  # Local, all external interfaces
				return set(self.transports.keys()) - set([0])
			else:
				return set([0])

		ret = set()
		for fileno, tgl in self.transportGroupLists.iteritems():
			for dgroup in msg.dstgroups:
				if dgroup in tgl.txGroups:
					ret.add(fileno)
		return ret


	def transportAdded(self, transport):
		""" When a transport comes up, add it to our list """
		newtgl = TransportGroupList(self.msgintf, transport.fileno()) 
		self.transportGroupLists[transport.fileno()] = newtgl

		# Remember, rxGroups = Union(all other txgroups), need to rebuild this one as its blank right now
		for othertgl in self.transportGroupLists.itervalues():
			if othertgl is newtgl: continue
			newtgl.rxGroups.incGroup(othertgl.txGroups.keys())

		log.info("added transport %s, init rxgroup to: %s", transport, newtgl.rxGroups)
		# ask neighbors to resend
		resend = MAGIMessage(contenttype=MAGIMessage.YAML, groups=[GroupRouter.ONEHOPNODES], docks=[GroupRouter.DOCK], data=yaml.safe_dump({'resend':True}))
		resend._routed = [transport.fileno()]
		log.info("Sending transport add message %s", resend)
		self.msgintf.send(resend)

		if len(newtgl.rxGroups) > 0:
			log.debug("Sending a group list out new transport as it initialized to nonzero")
			response = { 'set': list(newtgl.rxGroups), 'count': len(newtgl.rxGroups), 'checksum': listChecksum(newtgl.rxGroups) }
			listmsg = MAGIMessage(contenttype=MAGIMessage.YAML, groups=[GroupRouter.ONEHOPNODES], docks=[GroupRouter.DOCK], data=yaml.safe_dump(response))
			listmsg._routed = [transport.fileno()]
			self.msgintf.send(listmsg)


	def transportRemoved(self, fd, ignored):
		""" When a transport goes down, all subscriptions from that transport are considered dead """
		tgl = self.transportGroupLists[fd]
		log.debug("removing transport %s, rxGroups: %s", tgl, tgl.rxGroups)
		for othertgl in self.transportGroupLists.itervalues():
			if othertgl is tgl: continue
			othertgl.requestChanges([], tgl.txGroups)
		del self.transportGroupLists[fd]



