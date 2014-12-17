import logging
import time

from magi.util.execl import spawn, execAndRead
from magi.util.packetfiltering import FilterService

log = logging.getLogger(__name__)

class Counters():
	"""
		Class to read counter data via appropriate counter object and respond to requests
		This is an abstract class.  Subclasses for each OS deal with how to 
		read in the counter values.
	"""

	def __init__(self):
		self.filters = FilterService()
		self.mark2type = dict()  # map from integer handle to a (ip, type) tuple
		self.handlecounter = 500

	def mark2Type(self, mark):
		return self.mark2type.get(mark, None)

	def addDataCounter(self, counterName, **filters):
		for name in self.mark2type.itervalues():
			if name == counterName:
				log.warning("Not overriding counter %s, filters may not match", counterName)
				return
		self.mark2type[self.handlecounter] = counterName
		self.filters.addCounter(rulenum=self.handlecounter, **filters)
		self.handlecounter += 1

	def delDataCounter(self, counterName):
		for k, v in self.mark2type.items():
			if v == counterName:
				self.filters.deleteFilter(rulenum=k)
				del self.mark2type[k]
				return

	def loadCounters(self):
		""" Subclass needs to override to figure out how to read in counter data """
		return {}

	def clear(self):
		self.filters.deleteCounters()
		self.mark2type = dict()


class IPTablesCounters(Counters):
	""" Perform counter read using iptables on Linux 2.4 and 2.6 kernels """

	def __init__(self):
		Counters.__init__(self)

	def loadCounters(self):
		log.debug("Entering loadCounters")
		output = execAndRead("iptables -L -nvxt mangle")[0]
		log.debug("Done reading from iptables")

		state = None
		results = {}

		# Output for ipabltes v1.4.4
		# 0   1   2   3	  4	    5	   6		   7		   8	9	  10   11   12	  13   
		# 0   0  all  --  ifin  ifout  0.0.0.0/0   0.0.0.0/0   MARK match !0x4
		# 0   0  all  --  ifin  ifout  0.0.0.0/0   0.0.0.0/0   MARK match !0x5 DSCP match 0x38

		# Output for iptables v1.4.12 (note the whitespace)
		# 0   0  all  --  ifin  ifout  0.0.0.0/0   0.0.0.0/0   mark match !    0x1f4
		# 0   0  all  --  ifin  ifout  0.0.0.0/0   0.0.0.0/0   mark match !    0x1f5


		for line in output.splitlines():
			if line.startswith("Chain POSTROUTING"):
				state = "output"
				continue
			elif line.startswith("Chain PREROUTING"):
				state = "input"
				continue
			elif line.startswith("Chain"):
				state = None 
				continue

			if state is None:  # Not looking at a chain we care about
				continue

			if "mark match" not in line.lower():  # not a data line
				continue

			data = line.split()
			if data[10].startswith('!0x'):
				mark = int(data[10][1:], 16)
			else:
				mark = int(data[11], 16)
			key = self.mark2Type(mark)
			if key is not None:
				results[key] = (data[0], data[1])

		return results



class IPFWCounters(Counters):
	""" Perform counter read using IPFW on FreeBSD kernels """

	def __init__(self):
		Counters.__init__(self)

	def loadCounters(self):
		output = execAndRead("/sbin/ipfw show")[0]
		results = {}

		# 00500	   8	   596 count ip from any to any layer2 in via em0
		# 00501  334308 146275692 count ip from any to any layer2 in via em2
		# 00502	  25	  2024 count ip from any to any layer2 in via em3

		for line in output.splitlines():

			data = line.split(None, 4)
			mark = int(data[0], 10)  # don't let it try oct or hex, its base 10
			key = self.mark2Type(mark)
			if key is not None:
				results[key] = (data[1], data[2])

		return results


class NPFCounters(Counters):
	"""
	 NPF differs in a few ways:
	  1. Single mingw (python is using cyginw lib) application reads and reports them all
	  2. no discernable MAC names so we use IP addresses
	  3. override addCounter methods as filters service doesn't deal with this method
	"""

	def __init__(self):
		import socket
		self.cmd = "winstat.exe "
		Counters.__init__(self)
		spawn(self.cmd)
		time.sleep(0.5)

		# Connect to the winstat socket for reading
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect(('127.0.0.1', 2345))
		self.fp = sock.makefile()

	def addDataCounter(self, counterName, **filters):
		self.mark2type[self.handlecounter] = counterName
		filters['handle'] = self.handlecounter
		self.fp.write("%s\n" % ','.join("%s=%s" % (k, filters[k]) for k in filters))
		self.fp.flush()
		self.handlecounter += 1

	def delDataCounter(self, counterName):
		for k, v in self.mark2type:
			if v == counterName:
				self.fp.write("-%d\n" % k)
				self.fp.flush()
				return

	def loadCounters(self):
		self.fp.write('\n') # Trigger a read from winstat
		self.fp.flush()
		line = self.fp.readline().strip()
		results = {}

		# handleA:0:0 handleB:0:0 ...
		for counter in line.split():
			(mark, pkts, bytes) = map(int, counter.split(':'))
			key = self.mark2Type(mark)
			if key is not None:
				results[key] = (pkts, bytes)
			
		return results

