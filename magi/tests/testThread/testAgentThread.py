#!/usr/bin/env python
import logging

from magi.messaging.api import MAGIMessage

log = logging.getLogger("testagent")

def getAgent(**kwargs):
	return HTTPAgent()

class HTTPAgent(object):
	def run(self):
		msg = self.messenger.next()
		if msg.data == 'testdata':
			self.messenger.send(MAGIMessage(nodes='somewhere', docks='hello', data="response from testAgentThread"), acknowledgement=True, timestamp=98765)

	def stop(self):
		pass


