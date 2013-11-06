# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import time
import threading
import signal
import errno
import os
import Queue
import logging
import sys


log = logging.getLogger(__name__)

class PidKillService(object):
	"""
		The PidKillService provides a method for agents to offload the signal, wait, signal
		behaviour needed to stop a process and then intentially terminate if it doesn't stop
		on its own.  It runs in its own thread and keeps track of PIDs it is trying to kill
	"""

	DEPENDS = []
	SOFTWARE = []

	def __init__(self):
		self.queue = Queue.Queue()
		self.thread = PidKillThread(self.queue)
		self.thread.start()

	def kill(self, pids, sigtype):
		""" Send a single to the process id and then queue to check its status later """
		try:
			if type(pids) is not list:
				pids = [pids]

			for pid in pids:
				log.info("Killing pid %d" % (pid))
				os.kill(pid, sigtype)
				self.queue.put((pid, "pid", time.time()))
		except OSError, e:
			if e.args[0] != errno.ESRCH:  # no such process
				raise
		except:
			log.warning("Trouble killing process %d (%s)" % (pid, sys.exc_info()[1]))


	def killpg(self, gids, sigtype):
		""" Send a single to the process group and then queue to check its status later """
		try:
			if type(gids) is not list:
				gids = [gids]

			for gid in gids:
				log.info("Killing gid %d" % (gid))
				os.killpg(gid, sigtype)
				self.queue.put((gid, "group", time.time()))
		except OSError, e:
			if e.args[0] != errno.ESRCH:  # no such process
				raise
		except:
			log.warning("Trouble killing group %s (%s)" % (gids, sys.exc_info()[1]))


class PidKillThread(threading.Thread):

	def __init__(self, queue):
		self.queue = queue
		threading.Thread.__init__(self, name='pidkill')

	def run(self):
		# Items are queued with current time so first item is 'oldest'
		# This threads only purpose is to send a final KILL to things that won't die properly
		while True:
			ptuple = None
			try:
				ptuple = self.queue.get()
				
				# Less than 2 seconds have elapsed since queued
				while ptuple[2]+2 > time.time():
					time.sleep(0.5)

				log.info("pidkill, final kill for %d" % (ptuple[0]))
				try:
					if ptuple[1] == "group":
						os.killpg(ptuple[0], signal.SIGKILL)
					else:
						os.kill(ptuple[0], signal.SIGKILL)
				except OSError, e:
					if e.args[0] not in (errno.ESRCH, errno.ECHILD):
						raise
				
				try:
					os.waitpid(ptuple[0], os.WNOHANG) # only this thread calls waitpid
				except OSError, e:
					if e.args[0] not in (errno.ESRCH, errno.ECHILD):
						raise
			except:
				log.error("Trouble with final kill for %s (%s)" % (ptuple, sys.exc_info()[1]))


