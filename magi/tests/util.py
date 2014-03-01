
from magi.messaging.api import MessageStatus, MAGIMessage
from magi.messaging.transport import Transport
from magi.util.Collection import namedtuple
from magi.util.execl import pipeIn
from magi.testbed.base import Testbed, NIE, IFObj
from magi.testbed import testbed

from collections import defaultdict
import magi.util.execl
import magi.modules

import unittest2
import Queue
import yaml
import re
import os
import threading
import time
import logging
import sys

if 'installedAllowed' not in locals():
	ToTransmit = namedtuple("ToTransmit", "msg, fds, args")
	Transmitted = namedtuple("Transmitted", "msg, args")
	installAllowed = None

def softwareRequired(name):
	if os.geteuid() != 0:  # no class decorators in 2.5 and below
		raise unittest2.SkipTest("Test '%s' requires software and you don't have root privleges, skipping." % name)

	global installAllowed
	if installAllowed is None:
		installAllowed = raw_input("Will need to install software to continue testing %s, is this okay [y/n]?" % name)

	if installAllowed != 'y':
		raise unittest2.SkipTest("Skipping %s test that requires software" % name)


class TestMessageIntf(object):  # for processors
	def __init__(self):
		self.incoming = []
		self.outgoing = []
		self.status = []
		self.pushmap = {}

	def send(self, msg):
		self.outgoing.append(msg)

	def sendDirect(self, msg):
		self.outgoing.append(msg)

	def messageStatus(self, txt, isack, msg):
		self.status.append(MessageStatus(txt, isack, msg))

	def needPush(self, queuename, when):
		self.pushmap[queuename] = when



class TestTransport(Transport):
	def __init__(self, fd):
		Transport.__init__(self)
		self.myfileno = fd
		self.socket = None

	def fileno(self):
		return self.myfileno

	def __repr__(self):
		return "TestTransport %d" % self.myfileno
	__str__ = __repr__


class SimpleMessaging(object):
	def __init__(self):
		self.outgoing = Queue.Queue()
		self.incoming = Queue.Queue()
		self.groups = defaultdict(set)

	def poisinPill(self):
		self.incoming.put("PoisinPill")

	def nextMessage(self, block=False, timeout=None):
		return self.incoming.get(block, timeout)

	def send(self, msg, **args):
		self.outgoing.put(Transmitted(msg, args))

	def join(self, group, flag="default"):
		self.groups[group].add(flag)

	def leave(self, group, flag="default"):
		self.groups[group].discard(flag)
		if len(self.groups[group]) == 0:
			del self.groups[group]

	def trigger(self, **kwargs):
		self.send(MAGIMessage(groups="control", docks="control", data=yaml.dump(kwargs), contenttype=MAGIMessage.YAML))

	def inject(self, msg):
		""" Inject a message that the user will see as an incoming message """
		self.incoming.put(msg)

	def extract(self, block=False, timeout=None):
		""" Extract the next message that the user send through the messaging system """
		return self.outgoing.get(block, timeout)

	def checkMembership(self, group):
		""" Check to see if the messaging system things it is a memeber of group X """
		return len(self.groups[group]) > 0
	
	def stop(self):
		pass


class TestMessagingWrapper(object):
	""" Wraps other components to provide a common interface to threaded agents """
	def __init__(self, dock):
		self.rxqueue = Queue.Queue()
		self.txqueue = Queue.Queue()
		self.dock = dock

	def next(self, block=True, timeout=None):
		return self.rxqueue.get(block, timeout)

	def trigger(self, **kwargs):
		self.send(MAGIMessage(groups="control", docks="control", data=yaml.dump(kwargs), contenttype=MAGIMessage.YAML))

	def send(self, msg, **kwargs):
		return self.txqueue.put(msg)

	def poisinPill(self):
		self.rxqueue.put("PoisinPill")


class AgentFixture(threading.Thread):
	""" Wrapper used to interface with agents under test """
	def __init__(self, agentmodule, dock, args):
		threading.Thread.__init__(self, name='agentfixture')
		self.daemon = True
		self.agent = getattr(agentmodule, 'getAgent')()
		self.args = args
		self.messenger = TestMessagingWrapper(dock)

	def inject(self, msg):
		""" Inject a message that the agent will see as an incoming message """
		self.messenger.rxqueue.put(msg)

	def extract(self, block=False, timeout=None, onlyuntil=None):
		""" Extract the next message that the agent sent """
		if onlyuntil is not None:
			return self.messenger.txqueue.get(True, onlyuntil - time.time())
		return self.messenger.txqueue.get(block, timeout)

	def run(self):
		self.agent.run(self.messenger, self.args)

	def stop(self):
		self.agent.stop()


class AgentUnitTest(unittest2.TestCase):
	""" Install some standard pieces for the test case so it isn't repeated in all places """
	
	@classmethod
	def setUpClass(cls):
		if hasattr(cls, 'requirements'):
			cls.requirements()
	
		if len(testbed.getLocalIPList()) == 0:
			cls.mytestbed = UnitTestTestbed()
			cls.mytestbed.addIf('eth0', '192.168.2.149', '080027db588a', '255.255.255.0')
			testbed.setSubject(cls.mytestbed)

		magi.util.execl.execDebug = False # make sure its returned to normal
		magi.util.execl.execCalls = []

		cls.fixture = AgentFixture(cls.AGENT, cls.AGENT.__name__, [])
		cls.fixture.start()

	@classmethod
	def idltest(cls, path):
		def testIDL(self):
			allowed = ("name", "group", "display", "description", "inherits", "execute", "mainfile", "methods", "types", "datatables", "datatypes", "variables", "software")
			fp = open(path)
			idl = yaml.load(fp)
			# Things that must be there
			self.assertIn("name", idl)
			self.assertIn("display", idl)
			self.assertIn("description", idl)
			self.assertIn("execute", idl)
			self.assertIn("mainfile", idl)
			# make sure everything is okay
			for k in idl: 
				self.assertIn(k, allowed)
			fp.close()
		testIDL.__doc__ = "Make sure IDL for %s is parsable for formatted properly" % os.path.basename(path)
		return testIDL

	@classmethod
	def tearDownClass(cls):
		# shut it down nicely so we don't throw execptions from things disappearing
		cls.fixture.stop()
		cls.fixture.join(5.0)

	@classmethod
	def agentMain(cls, level=logging.DEBUG):
		hdlr = logging.StreamHandler()
		hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
		root = logging.getLogger()
		root.handlers = []
		root.addHandler(hdlr)
		root.setLevel(level)
		unittest2.main(verbosity=2)




class UnitTestTestbed(Testbed):
	""" Testbed instance for use when running unittests, lets us customize what the calling code sees """

	def __init__(self):
		Testbed.__init__(self)
		self.data = {
			'experiment': 'unittest2',
			'project': 'project',
			'eid': 'unittest2/project',
			'name': 'mynode'
		}

		self.iflist = list()

	def addIf(self, name, ip, mac, mask):
		self.iflist.append(IFObj(ip, name, mac, mask))

	def loadLocal(self):
		name = None
		for line in pipeIn('ifconfig').readlines():
			if 'thernet' in line:
				m = re.search('^(\w+).*([0-9a-fA-F:]{17})', line)
				if m is None: continue
				name = m.group(1)
				mac = m.group(2)

			if name is not None and 'inet addr' in line:
				m = re.search('addr:([0-9\.]+).*ask:([0-9\.]+)', line)
				if m is None: continue
				self.iflist.append(IFObj(m.group(1), name, mac, m.group(2)))
				name = None

	def getExperiment(self): return self.data['experiment']
	def getProject(self): return self.data['project']
	def getExperimentID(self): return self.data['eid']
	def getNodeName(self): return self.data['name']
	def getControlIP(self): NIE()
	def getControlIF(self): NIE()
	def getLocalIPList(self): return [obj.ip for obj in self.iflist]
	def getLocalIFList(self): return [obj.name for obj in self.iflist]
	def getInterfaceList(self): return self.iflist
	def getInterfaceInfo(self, ip):
		for i in self.iflist:
			if i.ip == ip:
				return i
		return IFObj(ip, 'unknown', 'FF:FF:FF:FF:FF:FF', '255.255.255.255')

