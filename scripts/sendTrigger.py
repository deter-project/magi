#!/usr/bin/env python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

# magi
import signal, time, sys, logging, yaml
import optparse
from magi.messaging import api

import subprocess

signal.signal(signal.SIGINT, signal.SIG_DFL)
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    optparser = optparse.OptionParser()
    optparser.add_option("-c", "--control", dest="control", help="The control node to connect to (i.e. control.exp.proj)", default="127.0.0.1") 
    optparser.add_option("-t", "--trigger", dest="trigger", help="The trigger to inject into the system.")
    optparser.add_option("-a", "--args", dest="args", help="Extra args to add to the trigger. Format: comma separated key=val pairs without spaces, \"foo=bar,123=onewtwothree\"")
    optparser.add_option("-v", "--verbose", dest="verbose", help="Tell orchestrator to print info about what its doing", default=False, action="store_true")
    optparser.add_option("-n", "--tunnel", dest="tunnel", help="Tell orchestrator to tunnel data through Deter Ops (users.deterlab.net).", default=False, action="store_true")
    (options, args) = optparser.parse_args()

    if not options.trigger:
        log.critical('Must give the trigger via -t or --trigger.')
        sys.exit(1)

    # Connect to backend
    log.info("Connecting to %s" % options.control)

    p = None
    if options.tunnel:
        localport = 18803
        p = subprocess.Popen("ssh users.deterlab.net -L " + str(localport) + ":" + options.control + ":18808 -N", shell=True)
        time.sleep(2)
        connection = api.ClientConnection("sendTrigger", "127.0.0.1", localport)
    else:
        connection = api.ClientConnection("sendTrigger", options.control, 18808)

    # get/send trigger events
    connection.join('trigger')

    # Should wait for some type of confirmation here instead
    # of assuming it worked.
    time.sleep(2)

    args = dict()
    if options.args: 
        args = dict([pair.split('=') for pair in options.args.split(',')])

    connection.trigger(event=options.trigger, **args)
    log.info('Sent trigger %s to Magi message transport via %s' % (options.trigger, options.control))

    # log.info("Trigger sent. Waiting for confirmation...")
    # while True:
    #     # block until next message
    #     raw_msg = connection.nextMessage(True)
    #     msg_data = yaml.load(raw_msg.data)
    # 
    #     if options.verbose:
    #         log.debug('Got Message:')
    #         log.debug("%s -> group:%s, docks:%s" % (raw_msg.src, raw_msg.dstgroups, raw_msg.dstdocks))
    #         log.debug('Message data:%s' % raw_msg.data)

    #     if 'event' in msg_data and msg_data['event'] == options.trigger:
    #             break
    # 
    # log.debug('confirmed.')

    time.sleep(3)

    if p:
        p.terminate()
