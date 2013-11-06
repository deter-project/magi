#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import sys
import logging
from magi.testbed import testbed
from magi.util.execl import run


log = logging.getLogger(__name__)

"""
  RoutingService is used to insert network routes and local redirects from an abstracted
  view and remove all of the inserted routes when asked.
"""

class RoutingService(object):
	""" 
		Provides access for adding/removing routes on the local system
	"""

	def __init__(self):
		self.addRoute = self._no_addRoute
		self.addDirect = self._no_addDirect
		self.addLocal = self._no_addLocal
		self._delRoute = self._no_delRoute

		if sys.platform.startswith('linux'):
			self.addRoute = self._linux_addRoute
			self.addDirect = self._linux_addDirect
			self.addLocal = self._linux_addLocal
			self._delRoute = self._linux_delRoute
		elif sys.platform.startswith('freebsd'):
			self.addRoute = self._bsd_addRoute
			self.addDirect = self._bsd_addDirect
			self._delRoute = self._bsd_delRoute
		elif sys.platform.startswith('cygwin'):
			self.addRoute = self._windows_addRoute
			self.addDirect = self._windows_addDirect
			self._delRoute = self._windows_delRoute

		self.localremovecmds = dict()
		self.routeremovecmds = dict()
		self.fakehash = dict()

	def addRoute(self, cidr, nexthop):
		""" Add a route given the cidr and nexthop IP address """
		pass

	def addDirect(self, cidr, intf):
		""" Add a route for a CIDR that is directly connected to an interface """
		pass

	def addLocal(self, cidr):
		""" Add a local route redirect for the given CIDR, only works on Linux """
		pass

	def addFake(self, nodename, cidr):
		""" Assign a fake range of addresses to a single node from cidr """
		cidrset = self.fakehash.setdefault(nodename, set())
		if cidr in cidrset:
			log.error("CIDR %s already used in virtual network, ignoring" % (cidr))
		if nodename == testbed.nodename:
			self.addLocal(cidr)
		else:
			nexthop = testbed.getNodeRoutes().get(nodename, None)
			if nexthop is None:
				log.error("Failed to add Fake, can't find nexthop for %s" % nodename)
				return
			self.addRoute(cidr, nexthop)
		cidrset.add(cidr)

	def delFake(self, nodename, cidr):
		""" Remove the assigned fake range of addresses (cidr) from node """
		self.fakehash.setdefault(nodename, set()).discard(cidr)
		if cidr in self.localremovecmds:
			run(self.localremovecmds.pop(cidr))
		if cidr in self.routeremovecmds:
			run(self.routeremovecmds.pop(cidr))

	def getFake(self, nodename):
		""" Get the list of CIDR's that are assigned as fake addresses for the node """
		return list(self.fakehash.setdefault(nodename, set()))

	def delRoute(self, cidr):
		"""
			Checks to see if this is a route 'we' added in which case we use the cached
			remove command for clear(), otherwise we call the hidden delRoute
		"""
		for c in self.routeremovecmds:
			if c.equals(cidr):
				run(self.routeremovecmds[c])
				return
		self._delRoute(cidr)


	def clear(self):
		"""
			Clear all of the routes that we added to this node
		"""
		for cmd in self.localremovecmds.itervalues() + self.routeremovecmds.itervalues():
			run(cmd)
		self.localremovecmds = dict()
		self.routeremovecmds = dict()



	def _no_addRoute(self, cidr, nexthop):
		""" Add a route given the cidr and nexthop """
		raise NotImplementedError("RoutingService.addRoute not implemented on this system")

	def _no_delRoute(self, cidr):
		""" Delete a route given the cidr """
		raise NotImplementedError("RoutingService.delRoute not implemented on this system")

	def _no_addDirect(self, cidr, intf):
		""" Add a route for a CIDR that is directly connected to an interface """
		raise NotImplementedError("RoutingService.addDirect not implemented on this system")

	def _no_addLocal(self, cidr):
		""" Add a local route redirect for the given CIDR """
		raise NotImplementedError("RoutingService.addLocal not implemented on this system")




	def _windows_addRoute(self, cidr, nexthop):
		""" Add a route given the cidr and nexthop """
		cmd = ["/cygdrive/c/WINDOWS/system32/route", "add", cidr.basestr, "mask", cidr.maskstr, nexthop]
		run(cmd)
		cmd[1] = "delete"
		self.routeremovecmds[cidr] = cmd[0:-1]

	def _windows_delRoute(self, cidr):
		""" Delete a route given the cidr and nexthop """
		run(["/cygdrive/c/WINDOWS/system32/route", "delete", cidr.basestr, "mask", cidr.maskstr])

	def _windows_addDirect(self, cidr, intf):
		""" Add a route for a CIDR that is directly connected to an interface """
		ip4if = None
		for i in testbed.getInterfaceList():
			if i.name == intf:
				ip4if = i.ip
		cmd = ["/cygdrive/c/WINDOWS/system32/route", "add", cidr.basestr, "mask", cidr.maskstr, ip4if]
		run(cmd)
		cmd[1] = "delete"
		self.routeremovecmds[cidr] = cmd[0:-1]




	def _bsd_addRoute(self, cidr, nexthop):
		""" Add a route given the cidr and nexthop """
		cmd = ["route", "add", "%s/%s" % (cidr.basestr, cidr.maskbits), nexthop]
		run(cmd)
		cmd[1] = "delete"
		self.routeremovecmds[cidr] = cmd

	def _bsd_delRoute(self, cidr):
		run(["route", "delete", "%s/%s" % (cidr.basestr, cidr.maskbits)])

	def _bsd_addDirect(self, cidr, intf):
		""" Add a route for a CIDR that is directly connected to an interface """
		cmd = ["route", "add", "%s/%s" % (cidr.basestr, cidr.maskbits), "-interface", intf]
		run(cmd)
		cmd[1] = "delete"
		self.routeremovecmds[cidr] = cmd



	def _linux_addRoute(self, cidr, nexthop):
		""" Add a route given the cidr and nexthop """
		cmd = ["route", "add", "-net", cidr.basestr, "netmask", cidr.maskstr, "gw", nexthop]
		run(cmd)
		cmd[1] = "del"
		self.routeremovecmds[cidr] = cmd

	def _linux_delRoute(self, cidr):
		run(["route", "del", "-net", cidr.basestr, "netmask", cidr.maskstr])

	def _linux_addDirect(self, cidr, intf):
		""" Add a route for a CIDR that is directly connected to an interface """
		cmd = ["route", "add", "-net", cidr.basestr, "netmask", cidr.maskstr, "dev", intf]
		run(cmd)
		cmd[1] = "del"
		self.routeremovecmds[cidr] = cmd

	def _linux_addLocal(self, cidr):
		""" Add a local route redirect for the given CIDR """
		cmd = ["ip", "route", "add", "table", "local", "local", "%s/%d" % (cidr.basestr, cidr.maskbits),
				 "dev", "lo", "proto", "kernel"]
		run(cmd)
		cmd[2] = "del"
		self.localremovecmds[cidr] = cmd


