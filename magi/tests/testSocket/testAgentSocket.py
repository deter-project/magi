#!/usr/bin/env python

import logging
import sys

from magi.messaging.api import MAGIMessage
from magi.util.agent import Agent
from magi.util.processAgent import initializeProcessAgent

logging.basicConfig(level=0) #logging.DEBUG)
log = logging.getLogger("testagent")

if __name__ == '__main__':
	name = sys.argv[1]
	dock = sys.argv[2]
	args = sys.argv[3:]

	agent = Agent()
	initializeProcessAgent(agent, sys.argv)
	
	msg = agent.messenger.next()
	log.debug("message in is %s" % msg)

	if msg.data == 'testdata':
		log.debug("data is testdata, sending response")
		agent.messenger.send(MAGIMessage(nodes='somewhere', docks='hello', data="response from testAgentSocket"), acknowledgement=True, timestamp=98765)
	else:
		log.debug("data was %s, not testdata", msg.data)

	log.debug("message sending done")
	justwait = agent.messenger.next()
	agent.done = True

