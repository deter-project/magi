#!/usr/bin/env python

import logging
import yaml
import optparse
import signal
import time
import sys

from magi.orchestrator.parse import *
from magi.messaging import api

logging.basicConfig(level=logging.INFO)
done = False
messaging = None

def handler(signum, frame):
	global done
	print "shutting down ..."
	done = True
	messaging.poisinPill()

def doStartup(period=1, level=20):
	call = BaseMethodCall(groups="__ALL__", docks="logger", method="setConfiguration", args={'level':level})
	messaging.send(call.getMessages()[0])

	call = BaseMethodCall(groups="__ALL__", docks="daemon", method="loadAgent", args={'name':'log1', 'dock':'log1', 'code':'datastreamer', 'idl': 'logStreamer', 'execargs':[] })
	messaging.send(call.getMessages()[0])

	call = BaseMethodCall(groups="__ALL__", docks="log1", method="setConfiguration", args={'interval':period, 'dstgroups':['loglis'], 'dstdocks':['loglis']})
	messaging.send(call.getMessages()[0])

	call = BaseMethodCall(groups="__ALL__", docks="log1", method="startStream", args={})
	messaging.send(call.getMessages()[0])


def logdisplay(nodename, entry):
	print nodename, entry['thread'], entry['level'], entry['msg']


def doShutdown():
	call = BaseMethodCall(groups="__ALL__", docks="log1", method="stopStream", args={})
	messaging.send(call.getMessages()[0])
	time.sleep(1) # Todo: Add ack and wait for its return


if __name__ == '__main__':
	optparser = optparse.OptionParser() 
	optparser.add_option("-c", "--control", dest="control", help="The control node to connect to (i.e. control.exp.proj)")
	optparser.add_option("-l", "--level", dest="level", help="The log level to use, 10=debug, 20=info, 30=warning, 40=error", default=20)
	#optparser.add_option("-t", "--thread", dest="thread", help="The thread to restrict to", default=None)
	(options, args) = optparser.parse_args()

	signal.signal(signal.SIGINT, handler)
	messaging = api.ClientConnection("pylogs", options.control, 18808)
	messaging.join("loglis")  # same group as we ask log agent to stream to
	doStartup(level=int(options.level)) #, thread=options.thread)
	while not done:
		msg = messaging.nextMessage(True, sys.maxint)  # without at least some timeout main thread stops receiving signals
		if msg is not None and type(msg) is not str and 'loglis' in msg.dstdocks:
			logdata = yaml.load(msg.data)
			for nodename in logdata:
				for record in logdata[nodename]:
					logdisplay(nodename, record)

	doShutdown()



