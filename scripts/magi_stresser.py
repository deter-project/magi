#!/usr/bin/env python

import logging
import yaml
import optparse
import signal
import time

from magi.orchestrator import *
from magi.messaging import api

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
	optparser = optparse.OptionParser() 
	optparser.add_option("-c", "--control", dest="control", help="The control node to connect to (i.e. control.exp.proj)", default="127.0.0.1") 
	optparser.add_option("-s", "--size", dest="size", type="int", help="The size of the message packet. Default 1000Bytes ", default="1000") 
	optparser.add_option("-r", "--rate", dest="rate", type="float", help="The rate of the packets in packets per second. Default 100messages/sec", default="0.01") 
	(options, args) = optparser.parse_args()

	signal.signal(signal.SIGINT, signal.SIG_DFL)
	messaging = api.ClientConnection("pystresser", options.control, 18808)
	#messaging = api.SSLClientConnection("pystresser", options.control, 18810, "DeterTest", "bwilson-orch")
	messaging.join("data")
	time.sleep(2)
        
	options.rate = 1/options.rate	
	print options.size, options.rate 

	req  = {
		'version': 1.0,
		'method': 'loadAgent',
		'args': {
			'name':'stress1',
			'code':'stress',
			'dock':'stress1',
			'args':[],
		}
	}
	messaging.send(api.MAGIMessage(nodes="chapel", docks="daemon", contenttype=api.MAGIMessage.YAML, data=yaml.safe_dump(req)))

	req  = {
		'version': 1.0,
		'method': 'setConfiguration',
		'args': {
			'interval':options.rate,
			'size':options.size,
			'dstgroup':'data'
		}
	}
	messaging.send(api.MAGIMessage(nodes="chapel", docks="stress1", contenttype=api.MAGIMessage.YAML, data=yaml.safe_dump(req)))


	req  = { 'version': 1.0, 'method': 'startTest', 'args': {} }
	messaging.send(api.MAGIMessage(nodes="chapel", docks="stress1", contenttype=api.MAGIMessage.YAML, data=yaml.safe_dump(req)))


	stopat = time.time() + 60
	while time.time() < stopat:
		msg = messaging.nextMessage(True)
		#print msg

	req  = { 'version': 1.0, 'method': 'stopTest', 'args': {} }
	messaging.send(api.MAGIMessage(nodes="chapel", docks="stress1", contenttype=api.MAGIMessage.YAML, data=yaml.safe_dump(req)))

	time.sleep(2)
