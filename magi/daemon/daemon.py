#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import threading
import logging
import time
import tempfile
import base64
import cStringIO
import glob
import errno
import tarfile
import shutil

from subprocess import Popen, PIPE

from magi.daemon.externalAgentsThread import ExternalAgentsThread, PipeTuple
from magi.daemon.threadInterface import ThreadedAgent
from magi.messaging.api import *
from magi.util.calls import doMessageAction
from magi.util.agent import agentmethod
from magi.util.software import requireSoftware

import magi.modules

log = logging.getLogger(__name__)

class Daemon(threading.Thread):
	"""
		The daemon process that listens to the messaging system and dispatches messages according
		to their destination dock.  It will also handle messages for the dock 'daemon', these include 
		messages such as 'exec'.
	"""

	def __init__(self, hostname, transports, enable_dataman_agent=False):
		threading.Thread.__init__(self, name='daemon')
		# 9/16/2013 hostname is passed in from the magi_daemon script correctly 
		self.hostname = hostname
		self.messaging = Messenger(self.hostname)
		self.forever = list()

#		self.messaging.startDaemon()
		self.pAgentThread = ExternalAgentsThread(self.messaging)
		self.pAgentThread.start()
		self.pAgentPids = dict()
		self.staticAgents = list()  # statically loaded thread agents
		self.threadAgents = list()  # dynamically loaded thread agents

		if enable_dataman_agent:
			self.startAgent(code="dataman", name="dataman", dock="dataman", static=True)
		
		self.configureMessaging(transports)


	def configureMessaging(self, transports, **kwargs):
		"""
			Called by main process to setup the local messaging system with the necessary links.
		"""
		one = False
		for entry in transports:
			try: 
				clazz = entry.pop('class')
				conn = globals()[clazz](**entry)  # assumes we have imported from messaging.api.*
				self.messaging.addTransport(conn, True)
				one = True
			except Exception, e:
				log.error("Failed to add new transport %s: %s", entry, e)

		if not one:
			# Couldn't make any links, might as well just quit
			raise IOError("Unable to start any transport links, I am stranded")

			
	def stop(self):
		"""
			Called to shutdown the daemon nicely by stopping all agent threads, stopping external processes
			and stopping the messaging thread.
		"""
		self.done = True
		self.messaging.poisinPill()
		log.debug("Stopping process agent loop")
		self.pAgentThread.stop()
		for agent in self.staticAgents + self.threadAgents:
			log.debug("Stopping %s", agent)
			agent.stop()
		log.debug("Joining with process agent loop")
		self.pAgentThread.join(3.0)  # try and be nice and wait, otherwise just move along
		log.debug("Checking for active threaded agents")
		for agent in self.staticAgents + self.threadAgents:
			log.debug("Joining with %s", agent)
			agent.join(0.5) # again, try but don't wait around forever, we are quiting anyhow
		log.debug("daemon stop complete")


	def run(self):
		"""
			Daemon thread loop.  Continual processing of incoming messages while monitoring for the
			stop flag.
		"""
		self.done = False
		while not self.done:
			try:
				msg = self.messaging.nextMessage(block=True)
				if msg is None:
					continue
				if type(msg) is str and msg == 'PoisinPill': # don't cause conversion to string for every message
					break

				progress = False
				for dock in msg.dstdocks:
					if dock == 'daemon':
						log.log(5, "Handling daemon with local call")
						progress = True
						doMessageAction(self, msg, self.messaging)
		
					for tAgent in self.staticAgents + self.threadAgents:
						if dock in tAgent.docklist:
							log.log(5, "Handing %s off to threaded agent", dock)
							progress = True
							tAgent.rxqueue.put(msg)
		
					if self.pAgentThread.wantsDock(dock):
						log.log(5, "Handing %s off to pipes thread", dock)
						progress = True
						self.pAgentThread.fromNetwork.put(msg)

					if not progress:
						log.error("Unknown dock %s, nobody processed.", dock)

			except Exception, e:
				log.error("Problems in message distribution: %s", e, exc_info=1)
				time.sleep(0.5)


	@agentmethod()
	def ping(self, msg):
		"""
			Alive like method call that will send a pong back to the caller
		"""
		res = {
		        'pong': True
		}
		# Added a data part to the message otherwise it gets dropped by the local daemon itself 
		self.messaging.send(MAGIMessage(nodes=msg.src, docks='pong',contenttype=MAGIMessage.YAML, data=yaml.safe_dump(res)))

	@agentmethod()
	def getAgentsProcessInfo(self, msg):
		processId = os.getpid()
		result = []
		for tAgent in self.staticAgents + self.threadAgents:
			result.append({"name": tAgent.agentname, "processId": processId, "threadId": tAgent.tid})
		for name in self.pAgentPids.keys():
			result.append({"name": name, "processId": self.pAgentPids[name]})
		res = {
		        'result': result
		}
		self.messaging.send(MAGIMessage(nodes=msg.src, docks=msg.srcdock,contenttype=MAGIMessage.YAML, data=yaml.safe_dump(res)))
	
	@agentmethod()
	def joinGroup(self, msg, group, nodes):
		"""
			Request to join a particular group
		"""
		if self.hostname in nodes:
			self.messaging.join(group, "daemon")
			# 9/14: Changed testbed.nodename to self.hostname to support desktop daemons  
			self.messaging.trigger(event='GroupBuildDone', group=group, nodes=[self.hostname])

	@agentmethod()
	def leaveGroup(self, msg, group, nodes):
		""""
			Request to leave a particular group
		"""
		if self.hostname in nodes:
			self.messaging.leave(group, "daemon")
			# 9/14: Changed testbed.nodename to self.hostname to support desktop daemons  
			self.messaging.trigger(event='GroupTeardownDone', group=group, nodes=[self.hostname])


	@agentmethod()
	def unloadAll(self, msg):
		"""
			Call to unload all dynamically started agents, generally used for testing
		"""
		for tAgent in self.threadAgents:
			tAgent.stop()
		for tAgent in self.threadAgents:
			tAgent.join(0.5) # try but don't wait around forever

		# TODO: stop process agents as well

	@agentmethod()
	def unloadAgent(self, msg, name):
		'''
		Unload the named agent, if it's loaded. 
		'''
		unloaded = []
		for i in range(len(self.threadAgents)):
			if name == self.threadAgents[i].agentname:
				log.debug("Unloading agentname %s dock %s",self.threadAgents[i].agentname, self.threadAgents[i].docklist)
				self.threadAgents[i].stop()
				self.threadAgents[i].join(0.5)
				# 9/14 Changed testbed.nodename to self.hostname to support desktop daemons  
				self.messaging.trigger(event='AgentUnloadDone', agent=name, nodes=[self.hostname])
				unloaded.append(i)
		
		if len(unloaded):
			self.threadAgents[:] = [a for a in self.threadAgents if a.agentname != name]

		# now check for process agents. If we find a dock, send the unload message to the 
		# agent. If it is well behaving, it'll commit harikari after cleaning up its
		# resources. 
		if not len(unloaded):
			data = yaml.load(msg.data)
			log.debug('message data: %s (%s)', data, type(data))
			if not 'args' in data or not 'dock' in data['args']:
				log.warning('No dock given in agentUnload. I do not know how to contact the'
							' process agent to tell it to unload. Malformed or incomplete '
							'message for AgentUnload')
			else:
				dock = data['args']['dock']
				if not self.pAgentThread.wantsDock(dock):
					log.warning('unloadAgent for dock I know nothing about. Ignoring.')
				else:
					log.debug('Sending stop message to process agent.')
					call = {'version': 1.0, 'method': 'stop', 'args': {}}
					stop_msg = MAGIMessage(docks=dock, contenttype=MAGIMessage.YAML, 
										   data=yaml.safe_dump(call))
					self.pAgentThread.fromNetwork.put(stop_msg)

					# TODO: remove dock and cleanup the external agent data structures.
					#		or confirm that external agents thread correctly discovers
					#		the transport is down and cleans things up correctly. 
					del self.pAgentPids[name]
		
		return True
		
	@agentmethod()
	def loadAgent(self, msg, code, name, dock, tardata=None, path=None, execargs=None, idl=None):
		"""
			Primary use daemon method call to start agents as a thread or process
		"""
		# Safety check, don't overload dock from loadAgent
		for tAgent in self.staticAgents + self.threadAgents:
			if dock in tAgent.docklist:
				log.info("Agent %s already loaded on dock %s. Returning successful \"load\".", tAgent, dock)
				# Send complete anyhow, perhaps a flag to indicate already loaded, but don't stop event flow process
				# 9/14: changed testbed.nodename to self.hostname to support desktop daemos 
				self.messaging.trigger(event='AgentLoadDone', agent=name, nodes=[self.hostname])
				return

		if self.pAgentThread.wantsDock(dock):
			log.info("Agent already loaded on dock %s. Returning successful \"load\".", dock)
			# 9/14: changed testbed.nodename to self.hostname to support desktop daemons 
			self.messaging.trigger(event='AgentLoadDone', agent=name, nodes=[self.hostname])
			return

		# Start by extracting the tardata into the appropriate modules directory if provided

		# TODO: 5/28/2013 
		# code is the location of the directory where the agent module resides
		# Current the orch generates the code variable by concating agentname and word "code"
		#  However there are no checks, these should be present in the orch or here? 
		# if code is not specified, the tardata or path needs to be specified 
		# If code is not specified, then find out the code name from the idl. 
		#
		cachepath = os.path.join(magi.modules.__path__[0], code)
		if tardata is not None:
			self.extractTarBuffer(cachepath, tardata)
		elif path is not None:
			self.extractTarPath(cachepath, path)

		self.startAgent(code, name, dock, execargs, idl)


	# Internal functions

	def startAgent(self, code=None, name=None, dock=None, execargs=None, idl=None, static=False):
		# Now find the interface definition and load it
		try:
			log.debug('startAgent code: %s, idl: %s' % (code, idl))
			dirname = os.path.join(magi.modules.__path__[0], code)
			if idl:
				idlFile = dirname+'/%s.idl' % idl
			else:
				idlFile = glob.glob(dirname+'/*.idl')[0]
		except IndexError:
			log.debug("No valid interface file in %s", dirname) 
			raise OSError(errno.ENOENT, "No valid interface file found in %s" % dirname)

		log.debug('reading interface file %s...' % idlFile)

		fp = open(idlFile)
		interface = fp.read()
		fp.close()
		interface = yaml.load(interface)

		# If there are software dependencies, load them before loading the agent
		if 'software' in interface:
			for package in interface['software']:
				log.info('Loading required package %s for agent %s.', package, name)
				requireSoftware(package)

		# Based on the interface execution method, execute the agent
		execstyle = interface['execute']
		mainfile = os.path.join(dirname, interface['mainfile'])

		log.debug('Running agent from file %s' % mainfile)
		
		# GTL TODO: handle exceptions from threaded agents by removing
		# the agents and freeing up the dock(s) for the agent.

		if execstyle == 'thread':
			# A agent should know the hostname and its own name  
			agent = ThreadedAgent(self.hostname, name, mainfile, dock, execargs, self.messaging)
			agent.start()
			log.debug("Started agent thread %s", agent)
			if static:
				self.staticAgents.append(agent)
			else:
				self.threadAgents.append(agent)
			self.messaging.trigger(event='AgentLoadDone', agent=name, nodes=[self.hostname])
		else:
			# Process agent, use the file as written to disk
			# TODO Process agent need to know hostname 
			args = []
			if execargs and type(execargs) == dict:
				# I apologize for this abuse
				args = ['%s=%s' % (str(k), str(v)) for k,v in execargs.iteritems()]
			os.chmod(mainfile, 00777)
			# (stderr, stderrname) = tempfile.mkstemp(suffix='.stderr', prefix=name+"-", dir='/tmp/')
			stderrname = os.path.join('/tmp', name + '.stderr')
			stderr = open(stderrname, 'w')		# GTL should this be closed? If so, when?
			log.debug("Starting %s, stderr sent to %s", name, stderrname)
			if execstyle == 'pipe':
				args.append('execute=pipe')
				log.debug('running: %s', ' '.join([mainfile, name, dock] + args))
				agent = Popen([mainfile, name, dock] + args, close_fds=True, stdin=PIPE, stdout=PIPE, stderr=stderr)
				self.pAgentThread.fromNetwork.put(PipeTuple([dock], InputPipe(fileobj=agent.stdout), OutputPipe(fileobj=agent.stdin)))

			elif execstyle == 'socket':
				args.append('execute=socket')
				log.debug('running: %s', ' '.join([mainfile, name, dock] + args))
				agent = Popen([mainfile, name, dock] + args, close_fds=True, stderr=stderr)

			else:
				log.critical("unknown launch style '%s'", interface['execute'])
				return False
			
			self.pAgentPids[name] = agent.pid
			self.messaging.trigger(event='AgentLoadDone', agent=name, nodes=[self.hostname] )

	def extractTarPath(self, cachepath, path):
		if os.path.isdir(path):
			# Copy all files to cache
			# TODO: make our own recursive copy that overwrites
			if os.path.exists(cachepath):
				log.debug('Found existing dir, removing it.')
				shutil.rmtree(cachepath)

			if not os.path.exists(cachepath):
				log.debug("Copytree %s into %s", path, cachepath)
				shutil.copytree(path, cachepath) # fails if dir already exists
		else:
			# Assume file and extract appropriately
			log.debug("Extract %s into %s", path, cachepath)
			tar = tarfile.open(name=path, mode="r") 
			for m in tar.getmembers():
				tar.extract(m, cachepath)
			tar.close()


	def extractTarBuffer(self, cachepath, tardata):
		# cache data to file
		log.debug("Extracting source to %s", cachepath)
		if os.path.exists(cachepath):
			log.warning("%s already exists, overwriting", cachepath)
		else:
			os.mkdir(cachepath)

		# decode tardata into a temp file
		scratch = tempfile.TemporaryFile()
		sp = cStringIO.StringIO(tardata)
		base64.decode(sp, scratch)
		sp.close()

		# now untar that into the selected modules directory
		scratch.seek(0)
		tar = tarfile.open(fileobj=scratch, mode="r:") # don't allow tests for compression, broken on p24 w/ fileobj
		for m in tar.getmembers():
			tar.extract(m, cachepath)
		tar.close()

