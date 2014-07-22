#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from subprocess import Popen, PIPE, call
from signal import SIGKILL, SIGTERM
import time
import logging
import types
import sys
import platform, os

"""
	Need to abstract subprocess cals (see python bug 1731717)
	GTL: See http://bugs.python.org/issue1731717, the issue 
	has been fixed. 
"""

locallog = logging.getLogger(__name__)
execDebug = False
execCalls = list()

def needsplit(cmd, **kwargs):
	return (type(cmd) in types.StringTypes) and not kwargs.get('shell', False)

def procOpen(cmd, log = locallog, **kwargs): 
	proc = None
	log.debug("Spawning (%s)" % (cmd))
	if execDebug:
		execCalls.append(cmd)
		# I've got a feeling this is far from portable....
		return Popen(['sleep', '999999999'])
	if needsplit(cmd, **kwargs):
		proc = Popen(cmd.split(), **kwargs)
	else:
		proc = Popen(cmd, **kwargs)
	return proc

## Spawn a program and return the PID
def spawn(cmd, log = locallog, **kwargs):
	pid = -1
	log.debug("Spawning (%s)" % (cmd))
	if execDebug:
		execCalls.append(cmd)
		return 1
	if needsplit(cmd, **kwargs):
		pid = Popen(cmd.split(), **kwargs).pid
	else:
		pid = Popen(cmd, **kwargs).pid
	return pid


## Run a program and wait for it to finish
def run(cmd, log = locallog, **kwargs):
	ret = 0
	log.debug("Running (%s)" % (cmd))
	if execDebug:
		execCalls.append(cmd)
		return ret
	if needsplit(cmd, **kwargs):
		ret = call(cmd.split(), **kwargs)
	else:
		ret = call(cmd, **kwargs)
	return ret


## Exec a program and collect the stdout
def execAndRead(cmd, log = locallog, **kwargs):
	output = None
	err = None
	log.debug("Executing (%s)" % (cmd))
	if needsplit(cmd):
		(output, err) = Popen(cmd.split(), stdout=PIPE, stderr=PIPE, **kwargs).communicate()
	else:
		(output, err) = Popen(cmd, stdout=PIPE, stderr=PIPE, **kwargs).communicate()
	log.debug("Successfully executed (%s)" % (cmd))			
	if output is None:
		output = ""
	if err is None:
		err = ""
	return (output, err)


## Exec program and return a pipe for reading data
def pipeIn(cmd, log = locallog, **kwargs):
	log.debug("Spawning (%s)" % (cmd))
	input = Popen(cmd, shell=True, stdout=PIPE, **kwargs).stdout
	return input


## Exec program and return a pipe for sending data
def pipeOut(cmd, log = locallog, **kwargs):
	log.debug("Spawning (%s)" % (cmd))
	output = Popen(cmd, shell=True, stdin=PIPE, **kwargs).stdin
	return output


## Exec program and return a pipes for sending and reading data
def pipeBoth(cmd, log = locallog, **kwargs):
	log.debug("Spawning (%s)" % (cmd))
	p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, close_fds=True, **kwargs)
	input = p.stdout
	output = p.stdin
	return (input, output)

def adduser(username):
	if sys.platform.startswith('linux'):
		run(["useradd", username])
	elif sys.platform.startswith('freebsd'):
		run(["pw", "user", "add", username]) 
	else:
		raise NotImplementedError("adduser not implemented on this system")

def killDescendents(pid, log = locallog):
	'''
	Kill all the given descendents of the given pid. 
	'''
	log.debug('Killing descendents of pid %d' % pid)
	for cpid in getDescendents(pid):
		log.debug('SIGTERM to pid %d' % pid)
		os.kill(cpid, SIGTERM)

	time.sleep(1)  # Give them time to go gently into that good night.

	# TO DO	- only SIGKILL those that are still alive.
	for cpid in getDescendents(pid):
		os.kill(cpid, SIGKILL)

def getDescendents(passed_pid, log = locallog):
	'''
	Returns a set of all descendents of the given pid. 
	Does not use process group. It follows the pid->ppid tree.
	'''
	retVal = set()
	if platform.system() == 'Linux' or platform.system().find('BSD') != -1: 
		# log.debug('Searching for pid: %d', passed_pid)
		proc = Popen('ps -eao pid,ppid,args'.split(), stdout=PIPE)
		for line in proc.communicate()[0].splitlines()[1:]:   # Skip the header
			(pid,ppid,args) = line.split(None, 2)
			if int(ppid) == passed_pid:
				log.debug('Adding pid %s to ancestor list. argv: %s' % (pid, args))
				retVal.update([int(pid)])
				retVal.update(getDescendents(int(pid)))  # recurse to follow child pids.
	else:
		raise NotImplementedError('I am not familiar with the arguments to ps on this system.')

	return retVal				
