#!/usr/bin/env python

import sys
import logging
import yaml
import os
import socket
from magi.daemon.processInterface import AgentInterface
from magi.messaging.api import MAGIMessage
from magi.util.calls import dispatchCall
from magi.util.agent import agentmethod
from magi.testbed import testbed
from magi.util import config

log = logging.getLogger(__name__)

class ProcessDispatchAgent:
	def __init__(self):
		self.daemon_interface = None
		self.done = False
		self.execute = None   # one of 'pipe', 'socket'
		self.logfile = None
		self.magi_socket = None
		self.name = None

	def set_vars(self, argv):
		'''Look for key=value pairs. If this class instance has a self.variable 
		which matched key, set teh value to it. The set values are always strings.
		If you want a non-string type, you must coerse it yourself after calling this
		function.'''
		for arg in argv:
			words = arg.split('=')
			if len(words) == 2:
				log.debug('found key=value on command line.')
				if hasattr(self, words[0]):
					log.debug('setting self.%s = %s', words[0], words[1])
					setattr(self, words[0], words[1])

	def handle_argv(self, argv):
		'''argv is assumed to have the following format. (This is usually set by the
		Magi daemon):

			agent_name agent_dock execute=[pipe|socket] (logfile=path)

		Where agent_name and agent_dock are strings and the key in the key=value
		pairs is literally the key given. The value may be restricted.
		'''
		if len(argv) < 3:
			log.critical('command line must start with name and dock')
			sys.exit(2)

		self.name, dock = argv[1:3]
		args = argv[3:] if len(argv) > 3 else []

		self.set_vars(args)

		if not self.logfile:
			self.logfile = os.path.join('/tmp', self.name + '.log')

		handler = logging.FileHandler(self.logfile, 'w')
		handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s: '
											   '%(message)s', '%m-%d %H:%M:%S'))
		root = logging.getLogger()
		root.setLevel(logging.INFO)
		root.handlers = []
		root.addHandler(handler)

		log.info('argv: %s', argv)
		
		self.commPort = config.getConfig().get('processAgentsCommPort')
		if not self.commPort:
			self.commPort = 18809
			
		infd, outfd = self._getIOHandles()
		self.daemon_interface = AgentInterface(infd, outfd, blocking=True)

		# Tell the daemon we want to listen on the dock. 
		# GTL - why doesn't the Daemon just associate the dock
		# with this process?
		self.daemon_interface.listenDock(dock)

		# now that we're connected, send an AgentLoaded message. 
		args = {'event': 'AgentLoadDone', 'name': self.name, 'nodes': [testbed.nodename]}
		self.daemon_interface.trigger(**args)

	def run(self):
		'''
			This method does not return and will run until read error from Magi
			Daemon or self.done == True. 

			Process incoming messages from Magi Daemon and invoke methods on 
			derived class. This should be called from derived class "main". E.g.:

			if __name__ == "__main__": 
				agent = MyDispatchProcessAgent()
				agent.handle_argv(sys.argv)
				agent.run()

		'''
		while True:
			msg = self.daemon_interface.next()
			log.debug('got msg: %s', msg)
			if isinstance(msg, MAGIMessage): 
				self._doMessageAction(msg)

	def _doMessageAction(self, msg):
		data = yaml.load(msg.data)
		log.debug('dispatching msg: %s', msg)
		log.debug('dispatching data: %s', data)
		if 'method' in data:
			retVal = dispatchCall(self, msg, data)
	
			if retVal != None:
				if 'trigger' in data:
					self.daemon_interface.trigger(event=data['trigger'], 
								   nodes=[testbed.nodename], retVal=[retVal])
				else:
					self.daemon_interface.trigger(event='retVal', name=data['method'], 
								   nodes=[testbed.nodename], retVal=[retVal])
		else:
			log.warn('got message without supported (or any?) action')

	@agentmethod()
	def stop(self, msg): 
		'''Implments a default unloadAgent method. It just exits and does *no* resource
		cleanup. If a process agent uses resources it should reimplment this to cleanup
		its resources, then exit.'''
		log.warning('Got agent unload message. Shutting down.')
		self.daemon_interface.trigger(event='AgentUnloadDone', name=self.name, nodes=[testbed.nodename])
		sys.exit(0)

	def _getIOHandles(self):
		if not self.execute:
			log.error('not told communication channel (pipe, socket), assuming socket.')
			self.execute = 'socket'

		if self.execute == 'pipe':
			return sys.stdin.fileno(), sys.stdout.fileno()
		
		elif self.execute == 'socket':
			self.magi_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.magi_socket.connect(('localhost', self.commPort))
			return self.magi_socket.fileno(), self.magi_socket.fileno()
		else:
			log.critical('unknown execute mode: %s. Unable to continue.')
			sys.exit(3)


if __name__ == "__main__": 
	agent = MagiProcessAgent()
	agent.handle_argv(sys.argv)
	agent.run()
