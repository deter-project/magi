#!/usr/bin/env python

import logging
import yaml
import optparse
import signal
import sys
import Queue 
from time import sleep

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage

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
	optparser.add_option("-f", "--file", dest="file", help="file with events to send")
	(options, args) = optparser.parse_args()

	signal.signal(signal.SIGINT, handler)
	messaging = api.ClientConnection("orchTesterClient", "128.9.168.95", 18808)
        messaging.join('control')

        print "messaging up and ready"
   
	while not done:
            print "waiting for a message"
            try:
                msg =  messaging.nextMessage(block=True, timeout=1)
                if msg:
                    print "HELLO GOT", msg 
            except Queue.Empty:
                pass      

            nmsg = MAGIMessage(groups='control', contenttype=MAGIMessage.YAML, data=yaml.safe_dump({'data': 'lovely sent from client'}))
            messaging.send(nmsg)
            print nmsg




