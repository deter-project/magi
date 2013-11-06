#!/usr/bin/env python

import logging
import yaml
import optparse
import signal
import time
import sys

from random import choice
from string import ascii_lowercase

from magi.orchestrator.parse import *
from magi.messaging import api

logging.basicConfig(level=logging.INFO)
done = False
messaging = None
agentId = None
streamId = None

def handler(signum, frame):
	global done
	print "shutting down ..."
	done = True
	messaging.poisinPill()


def doStartup(agent, columns, period=1):
	call = BaseMethodCall(groups="__ALL__", docks="daemon", method="loadAgent", args={'name':agentId, 'dock':agentId, 'code':'datastreamer', 'idl': agent, 'execargs':[] })
	messaging.send(call.getMessages()[0])

	args = {'interval':period, 'dstgroups':streamId, 'dstdocks':streamId}
	if columns:
		args['cols'] = columns
	
	print 'args:', args
	call = BaseMethodCall(groups="__ALL__", docks=agentId, method="setConfiguration", args=args)
	messaging.send(call.getMessages()[0])

	call = BaseMethodCall(groups="__ALL__", docks=agentId, method="startStream", args={})
	messaging.send(call.getMessages()[0])


def doShutdown(unload):
	print 'stopping stream...'
	call = BaseMethodCall(groups="__ALL__", docks=agentId, method="stopStream", args={})
	messaging.send(call.getMessages()[0])

	if unload:
		print 'unloading agent...'
		call = BaseMethodCall(groups="__ALL__", docks="daemon", method="unloadAgent", args={'name':agentId, 'dock':agentId})
		messaging.send(call.getMessages()[0])

	time.sleep(1) # Todo: Add ack and wait for its return


if __name__ == '__main__':
	optparser = optparse.OptionParser() 
	optparser.add_option("-c", "--control", dest="control", help="The control node to connect to (i.e. control.exp.proj)")
	optparser.add_option("-a", "--agent", dest="agent", help="The agent to stream from.")
	optparser.add_option("-u", "--unload", dest="unload", default=True, help="If True, unload the streaming agents on shutdown, else do not.")
	optparser.add_option("-C", "--columns", dest="columns", help="If given, only stream the data from the given columns. Requires knowledge of counter data. If not given all columns are streamed. Columns must be given as comma separated string.")
	(options, args) = optparser.parse_args()

	clientId = 'listener_' + ''.join(choice(ascii_lowercase) for x in range(10))
	agentId = options.agent
	streamId = 'stream_' + options.agent

	signal.signal(signal.SIGINT, handler)
	messaging = api.ClientConnection(clientId, options.control, 18808)
	cols = None if not options.columns else options.columns.split(',')
	doStartup(options.agent, cols)
	messaging.join(streamId)
	while not done:
		msg = messaging.nextMessage(True, sys.maxint)  # without at least some timeout main thread stops receiving signals
		if msg is not None and type(msg) is not str and streamId in msg.dstdocks:
			streamData = yaml.load(msg.data)
			for nodename in streamData:
				for record in streamData[nodename]:
					print "%s: %s" % (nodename, record)

	doShutdown(options.unload)



