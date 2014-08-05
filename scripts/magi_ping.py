#!/usr/bin/env python

import Queue 
import time 
import yaml
import optparse
import signal
import sys
import logging 

from magi.orchestrator import AAL 

from magi.messaging import api
from magi.messaging.magimessage import MAGIMessage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO) # why the need for a double request?

if __name__ == '__main__':
    optparser = optparse.OptionParser() 
    optparser.add_option("-b", "--bridge", default=None, dest="bridge", help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    optparser.add_option("-t", "--timeout", dest="timeout", default = "30", help="Number of seconds to wait to receive the ping reply from the nodes on the overlay")
    optparser.add_option("-f", "--file", dest="file", action="store", default = None, help="The yaml-based procedure file ")


    (options, args) = optparser.parse_args()
    if options.bridge is None:
            optparser.print_help()
            sys.exit(2)

    nodeset = set() 
    if options.file is not None:
            aaldata =  yaml.load(open(options.file, 'r')) 
            for name, nodes in aaldata['groups'].iteritems():
                log.info("Adding nodes from group %s", name) 
                nodeset.update(nodes) 

    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL ) 

    # Join the overlay at the specified bridge node. 
    #Port is hard coded :(
    #TODO: read the msdl to find a bridge 
    messaging = api.ClientConnection("ping", options.bridge, 18808)

    # Create a ping message and send on the overlay 
    # All node on the overlay will receive it and the daemon will respond with a pong message 
    msg = MAGIMessage(groups=['__ALL__'],docks='daemon', contenttype=MAGIMessage.YAML, data=yaml.safe_dump({'method': 'ping', 'version': 1.0}))
    messaging.send(msg)

    # Wait for timeout seconds before stoppping 
    start = time.time()
    stop = start + int(options.timeout) 
    current = start

    # Wait in a loop for timeout seconds 
    while current < stop: 
        current = time.time()
        try:
            msg = messaging.nextMessage(True, timeout=1)
            if msg.src is not 'ping':
                log.info('%s: Node %s on magi overlay' % ( current, msg.src))
                if len(nodeset) > 0:
                    nodeset.remove(msg.src)
        # If there are no messages in the Queue, just wait some more 
        except Queue.Empty:
            pass 

    if len(nodeset) > 0:
        for n in nodeset:
           log.info("Did not receive ping from %s", n)


