# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from magi.util.execl import run
import logging
import sys

log = logging.getLogger(__name__)

class FilterService(object):
	""" Provide access to the local system packet filtering """
	
	DEPENDS = []
	SOFTWARE = []

	def __init__(self):
		self.rulenum = 500
		self.used = set()
		self.counters = set()
		self.markers = set()
		self.blocks = set()

		if sys.platform.startswith('linux'):
			self.filters = NetfilterFilters()
		elif sys.platform.startswith('freebsd'):
			self.filters = IPFWFilters()
		else:
			self.filters = NoFilters()
	
	def _pickrulenum(self, request):
		"""
			If a number is specified use that, other wise increment counter until we find 
			a number that hasn't been used yet.
		"""
		if request is None:
			request = self.rulenum
			while request in self.used:
				request += 1
			self.rulenum = request + 1
		self.used.add(request)
		return request

	def deleteFilter(self, rulenum):
		"""
			Delete the filter with the associated rulenum
		"""
		if rulenum not in self.used:
			log.error("Asked to delete a rulenum that doesn't exist")
			return

		self.used.discard(rulenum)
		self.counters.discard(rulenum)
		self.markers.discard(rulenum)
		self.blocks.discard(rulenum)

		self.filters.delete(rulenum)

	def deleteBlocks(self):
		""" Remove all blocking filters that we installed on this node """
		for n in self.blocks:
			self.filters.delete(n)
		self.blocks.clear()

	def deleteMarkers(self):
		""" Remove all marking filters that we installed on this node """
		for n in self.markers:
			self.filters.delete(n)
		self.markers.clear()

	def deleteCounters(self):
		""" Delete all counting filters that we installed on this node """
		for n in self.counters:
			self.filters.delete(n)
		self.counters.clear()

	def addInputBlockingFilter(self, rulenum=None, **kwargs):
		"""
			Add an input blocking rule and return the rulenum used.  If one is specified, it is used
			Potential arg keys:
			* dst <cidr> - destination address as a CIDR
			* src <cidr> - source address as a CIDR
			* proto <num> - protocol number
			* sport <num> - single source port as a number
			* dport <num> - single destination port as a number
		"""
		rulenum = self._pickrulenum(rulenum)
		self.filters.blockInput(rulenum=rulenum, **kwargs)
		self.blocks.add(rulenum)
		return rulenum

	def addOutputMarkingFilter(self, rulenum=None, **kwargs):
		"""
			Add a marking rule and return the rulenum used.  If a rulenum is specified, it is used
			Potential arg keys same as blocking filter plus:
			* marker 1-7, ipprecendece value to set on the matching packets

		"""
		rulenum = self._pickrulenum(rulenum)
		self.filters.markOutput(rulenum=rulenum, **kwargs)
		self.markers.add(rulenum)
		return rulenum

	def addCounter(self, rulenum=None, **kwargs):
		"""
			Add a counting rule and return the rulenum used.  If a rulenum is specified, it is used. Potential arg keys:
			* input <interface>, watch incoming interface
			* output <interface, watch outgoing interface
			* marker 0-7, watch traffic with marker num only
		"""
		rulenum = self._pickrulenum(rulenum)
		self.filters.addCounter(rulenum=rulenum, **kwargs)
		self.counters.add(rulenum)
		return rulenum



class NoFilters(object):
	""" Blank filter implementation for non supported operating systems """
	
	def init(self):
		pass

	def delete(self, rulenum):
		raise NotImplementedError("delete not implemented on this system")

	def blockInput(self, rulenum, **kwargs):
		raise NotImplementedError("blockInput not implemented on this system")

	def markOutput(self, rulenum, **kwargs):
		raise NotImplementedError("markOutput not implemented on this system")

	def addCounter(self, rulenum, **kwargs):
		raise NotImplementedError("addCounter not implemented on this system")



class IPFWFilters(NoFilters):
	""" Filter implementation for IPFW based systems such as FreeBSD """
	
	def __init__(self):
		""" ipfw show returns 0.  If its not loaded, it returns 71.  On any error, try and load the module and open the firewall """
		import subprocess
		NoFilters.__init__(self)
		if subprocess.call(["/sbin/ipfw", "show"]) != 0:
			subprocess.call(["/sbin/kldload", "/boot/kernel/ipfw.ko"]) 
			subprocess.call(["/bin/sh", "/etc/rc.firewall", "open"])

	def delete(self, rulenum):
		""" ipfw lets us delete using just the rulenum we provided """
		run(["/sbin/ipfw", "delete", str(rulenum)])

	def blockInput(self, rulenum, **kwargs):
		run(["/sbin/ipfw", "add", str(rulenum), "drop"] + self._filterargs(**kwargs))

	def addCounter(self, rulenum, **kwargs):
		cmd = ["/sbin/ipfw", "add", str(rulenum), "count", "ip", "from", "any", "to", "any"]
		if 'input' in kwargs:
			cmd.extend(["in", "via", kwargs['input']])
		else:
			cmd.extend(["out", "via", kwargs['output']])
		if 'marker' in kwargs:
			cmd.extend(["ipprecedence", str(kwargs['marker'])])
		run(cmd)

	def _filterargs(self, **kwargs):
		arglist = []	

		if ('proto' in kwargs):
			proto = str(kwargs['proto'])
		else:
			proto = "ip"

		if ('dst' in kwargs):
			dst = "%s/%d" % (kwargs['dst'].basestr, kwargs['dst'].maskbits)
		else:
			dst = "any"

		if ('src' in kwargs):
			src = "%s/%d" % (kwargs['src'].basestr, kwargs['src'].maskbits)
		else:
			src = "any"

		arglist.extend([proto, "from", src, "to", dst])

		if ('sport' in kwargs):
			arglist.extend(["src-port", str(kwargs['sport'])]);
		if ('dport' in kwargs):
			arglist.extend(["dst-port", str(kwargs['dport'])]);

		return arglist



class NetfilterFilters(NoFilters):
	""" Filter implementation for a Linux/Netfilter based system """

	def __init__(self):
		NoFilters.__init__(self)
		import subprocess
		subprocess.call(["iptables", "-F", "-t", "mangle"])
		subprocess.call(["iptables", "-F"])
		self.removelist = dict()

	def _add(self, cmd, rulenum):
		run(cmd)
		cmd[1] = "-D"
		self.removelist[rulenum] = cmd

	def delete(self, rulenum):
		"""
			Netfilter doesn't have 'global' rulenums so we need to remember the command we
			used to add the filter and use the same to delete it.
		"""
		try:
			run(self.removelist.pop(rulenum))
		except KeyError:
			log.error("Tried to remove rule %d, but it doesn't exist in list" % (rulenum))
		
	def blockInput(self, rulenum, **kwargs):
		self._add(["iptables", "-A", "INPUT", "-j", "DROP"] + self._filterargs(kwargs), rulenum)

	def markOutput(self, rulenum, **kwargs):
		self._add(["iptables", "-A", "OUTPUT", "-t", "mangle", "-j", "DSCP", "--set-dscp", str(kwargs['marker']<<3)] +
					 self._filterargs(kwargs), rulenum)

	def addCounter(self, rulenum, **kwargs):
		"""
			MARK isn't used as a target so the mark values are always 0.  Matching NOT mark means that this
			match is always true anded with the next matches.  We use it for id'ing our rule later on in a quick fashion
		"""
		cmd = ["iptables", "-A", "PREROUTING", "-t", "mangle", "-m", "mark", "!", "--mark", str(rulenum)]
		if 'output' in kwargs:
			cmd.extend(["-o", kwargs['output']])
			cmd[2] = "POSTROUTING"
		else:
			cmd.extend(["-i", kwargs['input']])
		if 'marker' in kwargs:
			cmd.extend(["-m", "dscp", "--dscp", str(kwargs['marker']<<3)])  # ipprecendence is top 3 bits of dscp
		self._add(cmd, rulenum)

	def _filterargs(self, kwargs):
		arglist = []	
		if ('dst' in kwargs):
			arglist.extend(["-d", "%s/%d" % (kwargs['dst'].basestr, kwargs['dst'].maskbits)])
		if ('src' in kwargs):
			arglist.extend(["-s", "%s/%d" % (kwargs['src'].basestr, kwargs['src'].maskbits)])
		if ('proto' in kwargs):
			arglist.extend(["-p", str(kwargs['proto'])])
		if ('sport' in kwargs):
			arglist.extend(["--sport", str(kwargs['sport'])]);
		if ('dport' in kwargs):
			arglist.extend(["--dport", str(kwargs['dport'])]);
		return arglist


