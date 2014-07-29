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


if __name__ == '__main__':
	optparser = optparse.OptionParser() 
	optparser.add_option("-c", "--control", dest="control", help="The control node to connect to (i.e. control.exp.proj)")
	optparser.add_option("-g", "--group", dest="group", help="One or more groups to join (control:data:testgrp)", default="ATestGroup")
	optparser.add_option("-n", "--nodes", dest="nodes", help="One or more nodes that should should join the group (node0:node1:control).")
	optparser.add_option("-x", "--exit", action='store_true', help="If given, tell everyone to leave the group when this script exits.", default=False)
	(options, args) = optparser.parse_args()

	signal.signal(signal.SIGINT, handler)
	messaging = api.ClientConnection("group_joiner", options.control, 18808)

	time.sleep(3)

	# This sets up the negihtbors for the clientConnection transport 
        if ':' in options.group:
       	    for grp in options.group.split(':'):
                print "Joining group:", grp
                messaging.join(grp)
        else:
      	    print "Joining group:", options.group
            messaging.join(options.group)



	nodes=options.nodes.split(':')

	# This sets up the negihtbors for the experiment network  transport 
	if ':' in options.group:
		for grp in options.group.split(':'):
			call = BuildGroupCall(grp, nodes)
			messaging.send(call.getMessages()[0])
	else:
		call = BuildGroupCall(options.group, nodes)
		messaging.send(call.getMessages()[0])

	while not done:
		msg = messaging.nextMessage(True, sys.maxint)
		print 'msg:', msg
		#if options.group in msg.dstgroups:
		#	print 'got msg:', msg

	if options.exit:
		call = LeaveGroupCall(options.group, nodes)
		messaging.send(call.getMessages()[0])
		time.sleep(1) # Todo: Add ack and wait for its return



