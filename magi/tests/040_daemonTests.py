#!/usr/bin/env python

import unittest2
import logging
import time
import base64
import yaml
import subprocess
import tempfile
import os
import sys
import cStringIO
import tarfile
from magi.util.calls import MethodCall, CallException, dispatchCall, doMessageAction
from magi.daemon.daemon import Daemon
from magi.messaging.api import MAGIMessage
from magi.tests.util import SimpleMessaging
import magi.tests


class DaemonTest(unittest2.TestCase):
	"""
		Testing of daemon and some of its components
	"""

	def setUp(self):
		subprocess.call("rm -rf %s" % (magi.modules.__path__[0] + '/testhttp*'), shell=True)
		trans = [ {'address': '127.0.0.1', 'class': 'TCPTransport', 'port': 51232} ]
		# trans = [ {'address': '239.255.1.2', 'class':' MulticastTransport', 'localaddr': '127.0.0.1', 'port': '18808'} ]
		self.d = Daemon('mynode', trans)
		self.d.daemon = True # Helps us exit easier
		self.q = SimpleMessaging()
		self.d.messaging = self.q
		self.d.pAgentThread.messaging = self.q
		self.d.start()

	def tearDown(self):
		self.d.stop()
		self.d.join(5.0)
		subprocess.call("rm -rf %s" % (magi.modules.__path__[0] + '/testhttp*'), shell=True)

	def test_MethodCall(self):
		""" Test MethodCall code """
		class tester(object):
			def __init__(self, ut):
				self.ut = ut
			def kwargs(self, msg, **kwargs):
				self.ut.assertEquals(kwargs['name1'], 'name1')
				self.ut.assertEquals(kwargs['name2'], 'name2')
				self.ut.assert_('name3' not in kwargs)
				self.ut.assertEquals(kwargs['ignoreme'], 'extra')
			def nameddefaults(self, msg, name1=None, name2=None, name3=None):
				self.ut.assertEquals(name1, 'name1')
				self.ut.assertEquals(name2, 'name2')
				self.ut.assertEquals(name3, None)
			def partdefaults(self, msg, name1, name2, name3=None):
				self.ut.assertEquals(name1, 'name1')
				self.ut.assertEquals(name2, 'name2')
				self.ut.assertEquals(name3, None)

		# decoding
		self.assertRaises(CallException, MethodCall, request={'version':2.0, 'method':'ping'})
		m = MethodCall(request={'version':1.0, 'method':'ping'})

		# call types
		call = {
			'version': 1.0,
			'method': 'kwargs',
			'args': { 'name1':'name1', 'name2':'name2', 'ignoreme':'extra' }
		}
		testobj = tester(self)
		msg=yaml.safe_dump(call)
		dispatchCall(testobj, msg, call)
		call['method'] = 'nameddefaults'
		msg = MAGIMessage(data=yaml.safe_dump(call))
		dispatchCall(testobj, msg, call)
		call['method'] = 'partdefaults'
		msg = MAGIMessage(data=yaml.safe_dump(call))
		dispatchCall(testobj, msg, call)
		del call['args']['name2']  # missing required argument
		msg = MAGIMessage(data=yaml.safe_dump(call))
		logging.disable(40)
		self.assertRaises(CallException, dispatchCall, testobj, msg, call)
		logging.disable(0)

		call['method'] = 'kwargs'
		call['args']['name2'] = 'name2'
		msg = MAGIMessage(data=yaml.safe_dump(call))
		doMessageAction(testobj, msg)

		call['trigger'] = 'MyAwesomeTrigger'
		msg = MAGIMessage(data=yaml.safe_dump(call))
		doMessageAction(testobj, msg, self.d.messaging)
	
	def test_Joiner(self):
		""" Test of join and leave requests """
		request = {
			'version': 1.0,
			'method': 'joinGroup',
			'args': {
				'group': 'supergroup',
				'nodes': ['mynode', 'n1', 'n2']
			}
		}

		self.q.inject(MAGIMessage(docks='daemon', data=yaml.safe_dump(request)))
		time.sleep(0.1)
		self.assert_(self.q.checkMembership('supergroup'))

		request['method'] = 'leaveGroup'
		self.q.inject(MAGIMessage(docks='daemon', data=yaml.safe_dump(request)))
		time.sleep(0.1)
		self.assert_(not self.q.checkMembership('supergroup'))


	def test_Alive(self):
		""" Test of alive requests """
		self.q.inject(MAGIMessage(src='othernode', docks='daemon', data=yaml.safe_dump({'version':1.0, 'method':'ping'})))
		self.assertEquals('pong', self.q.extract(True, 2).msg.dstdocks.pop())
		time.sleep(0.2)


	def test_ExecThread(self):
		""" Test execing of agent in thread """
		request = {
			'version': 1.0,
			'method': 'loadAgent',
			'args': { 'name': 'tht', 'code': 'testhttpthread', 'dock': 'HTTPT', 'execargs': {'arg1':1, 'arg2':2} }
		}
		self.doMessages(request, 'testThread', True)
			

	def test_ExecPipe(self):
		""" Test execing of agent in a separate process that connects over pipes """
		if 'ygwin' in sys.platform and sys.version_info[1] < 5:
			raise unittest2.SkipTest("Older cygwin can't do pipes")
		request = {
			'version': 1.0,
			'method': 'loadAgent',
			'args': { 'name': 'thp', 'code': 'testhttppipe', 'dock': 'HTTPP', 'execargs': {'arg1':1, 'arg2':2} }
		}
		self.doMessages(request, 'testPipe', False)


	def test_ExecSocket(self):
		""" Test execing of agent in a separate process that connects over a socket """
		request = {
			'version': 1.0,
			'method': 'loadAgent',
			'args': { 'name': 'ths', 'code': 'testhttpsocket', 'dock': 'HTTPC', 'execargs': {'arg1':1, 'arg2':2} }
		}
		self.doMessages(request, 'testSocket', False)


	def _waitForListen(self, dock, wait=5):
		""" Waits until a particular dock has a listening agent, i.e. wait for process to start, hacks into daemon parts """
		stopat = time.time() + wait
		while time.time() < stopat:
			for tAgent in self.d.threadAgents:
				if dock in tAgent.docklist:
					return
			if self.d.pAgentThread.wantsDock(dock):
					return
			time.sleep(0.1)

		self.assert_(False, "Failed to listen for dock withing %d seconds" % wait)


	def doMessages(self, request, dirname, needTrigger):
		store = cStringIO.StringIO()
		tar = tarfile.open(fileobj=store, mode='w')
		dirpath = os.path.join(magi.tests.__path__[0], dirname)
		os.chdir(dirpath)
		tar.add('.', recursive=True)
		tar.close()
		# store now has tar data

		listeningDock = request['args']['dock']

		# First attempt has no file and should fail, have to go direct so we can catch exception
		call = MethodCall(request=request)
		self.assertRaises(EnvironmentError, self.d.loadAgent, None, **call.args)

		# Second attempt has pathname and will run
		request['args']['path'] = dirpath
		self.doOneRequest(request, listeningDock, needTrigger)

		# Third attempt has tardata and will run
		del request['args']['path']
		request['args']['dock'] += '1'
		listeningDock += '1'
		request['args']['tardata'] = base64.encodestring(store.getvalue())
		subprocess.call("rm -rf %s" % (magi.modules.__path__[0] + '/testhttp*'), shell=True)
		self.doOneRequest(request, listeningDock, needTrigger)

		# Fourth attempt has no file but is cached and will run
		del request['args']['tardata']
		request['args']['dock'] += '2'
		listeningDock += '2'
		self.doOneRequest(request, listeningDock, needTrigger)


	def doOneRequest(self, request, listeningDock, needTrigger):
		# load agent request
		self.q.inject(MAGIMessage(docks='daemon', data=yaml.safe_dump(request)))
		self._waitForListen(listeningDock)

		if needTrigger:
			transmitted = self.q.extract(True, 2)
			self.assertIn('control', transmitted.msg.dstdocks)
			self.assertIn('control', transmitted.msg.dstgroups)

		# single test message
		self.q.inject(MAGIMessage(docks=listeningDock, data='testdata'))
		transmitted = self.q.extract(True, 2)
		self.assertEquals('hello', transmitted.msg.dstdocks.pop())
		self.assertEquals(True, transmitted.args['acknowledgement'])
		self.assertEquals(98765, transmitted.args['timestamp'])




if __name__ == '__main__':
	import signal
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)

