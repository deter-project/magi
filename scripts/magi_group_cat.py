#!/usr/bin/env python

import logging
import yaml
import optparse
import signal
import sys
from time import sleep

from magi.orchestrator.parse import *
from magi.messaging import api

log=logging.getLogger(__name__)

logging.basicConfig(level=logging.DEBUG)

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
	optparser.add_option("-g", "--group", dest="group", help="List one of more groups to listen to (ie \"control:data:trigger\") ", default="control")
	(options, args) = optparser.parse_args()

	signal.signal(signal.SIGINT, handler)
	messaging = api.ClientConnection("grcat", options.control, 18808)

	sleep(3)
   
	groups = options.group.split(':')
	for group in groups:
		messaging.join(group)

	
	sleep(20)
	while not done:
		line = sys.stdin.readline()
		if not line:
			break

		msg = MAGIMessage(groups=groups, contenttype=MAGIMessage.YAML, data=yaml.safe_dump({'data': line.rstrip('\n')}))
		messaging.send(msg)
		log.debug('sent msg: %s', msg)

                # Send a ping message to all the nodes" 
                messaging.ping()

	# wait for messages to be sent. Need to ad flush() to messaging.
	sleep(10)
		

