# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import logging
import threading
import imp
import Queue
import yaml
import sys
import traceback
from os.path import basename

import magi.modules
from magi.testbed import testbed
from magi.messaging.api import MAGIMessage

import os
import ctypes

log = logging.getLogger(__name__)

class MessagingWrapper(object):
	""" Wraps other components to provide a common interface to threaded agents """
	def __init__(self, name, messaging, inqueue, docklist):
		self.name = name
		self.messaging = messaging
		self.inqueue = inqueue
		self.docklist = docklist

	def next(self, block=True, timeout=None):
		""" Received the next message or a string "PoisinPill" if someone wants to wake up the waiting thread """
		return self.inqueue.get(block, timeout)

	def send(self, msg, **args):
		""" Send a message """
		return self.messaging.send(msg, **args)

	def joinGroup(self, group):
		""" Would like to see messages for group """
		return self.messaging.join(group, self.name)

	def leaveGroup(self, group):
		""" No longer care about messages for group, if another agent is still listening, the group will still be received """
		return self.messaging.leave(group, self.name)

	def listenDock(self, dock):
		""" Start listening for messages destined for 'dock' """
		self.docklist.add(dock)

	def unlistenDock(self, dock):
		""" Stop listening for messages destined for 'dock' """
		self.docklist.discard(dock)

	def trigger(self, **kwargs):
		self.send(MAGIMessage(groups="control", docks="daemon", data=yaml.dump(kwargs), contenttype=MAGIMessage.YAML))

	def poisinPill(self):
		""" queue a poisin pill so that anyone waiting on a call to next will wake up """
		self.inqueue.put("PoisinPill")


class ThreadedAgent(threading.Thread):
	"""
		The thread to run an agent in.  Uses the object returned by getAgent.  It will call the method *run*
		when the thread itself is started and *stop* when an external force requests that it stop.  Note that
		*stop* will be called from a different thread than the one running the agent.
	"""
	def __init__(self, hostname, name, sourcepath, dock, args, messaging):
		threading.Thread.__init__(self, name=name)
		self.setDaemon(True)
		log.debug("Loading source from %s with name %s", sourcepath, name)
		# TODO: check if this is the best way to get to the module name
		packagename = sourcepath.split("/")[-2]
		modulename = sourcepath.split("/")[-1].split(".")[0]
		__import__('magi.modules.'+packagename)
		module = imp.load_source('magi.modules.'+packagename+"."+modulename, sourcepath)

		self.agent = None
		self.agentname = name
		self.hostname = hostname 
		self.getAgent = getattr(module, 'getAgent')  # called to create a new agent, do it agent thread, not here
		self.docklist = set([dock])
		self.rxqueue = Queue.Queue()
		self.messaging = messaging
		self.args = args
		
	def run(self):
		try:
			self.pid = os.getpid()
			self.tid = ctypes.CDLL('libc.so.6').syscall(224) # TODO: Is this the right way to get to thread id
			try:
				# create the agent here, it may install software which is time consuming
				if self.args: 
					self.agent = self.getAgent(**self.args)
				else:
					self.agent = self.getAgent()

			except Exception, e:
				log.error("Agent %s on %s threw an exception %s during agent load.", self.getName(), self.hostname, e, exc_info=1)
				log.error("Sending back a RunTimeException event. This may cause the receiver to exit.")
				exc_type, exc_value, exc_tb = sys.exc_info()
				filename, line_num, func_name, text = traceback.extract_tb(exc_tb)[-1]
				filename = basename(filename)
				self.messaging.trigger(event='RuntimeException', func_name=func_name, agent=self.getName(), 
								   nodes=[testbed.nodename], filename=filename, line_num=line_num, error=str(e))
				return

			#9/16: Moved AgentLoadDone trigger to the daemon loadAgent call  
			#self.messaging.trigger(event='AgentLoadDone', name=self.agentname, nodes=[testbed.nodename])

			try:
				# send the load complete event to listeners
				# call the main run function
				self.agent.run(MessagingWrapper(self.agentname, self.messaging, self.rxqueue, self.docklist), self.args)
			except Exception, e:
				log.error("Agent %s on %s threw an exception %s during main loop", self.getName(), self.hostname, e, exc_info=1)
				# GTL TODO: do cleanup and useful things here!
		finally:
			log.info("Agent %s has finished", self.getName())

	def stop(self):
		try:
			if self.agent:
				self.agent.stop()
				# TOOD: get list of joined groups and leave them?, lower priority as we don't generally stop agents
		except Exception, e:
			log.error("Agent %s threw an exception %s when stopping.", self.getName(), e, exc_info=1)

