#!/usr/bin/env python

# Copyright (C) 2013 University of Southern California.
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from magi.util.agent import TrafficClientAgent
from magi.util.processAgent import initializeProcessAgent
from magi.util.distributions import *

import logging
import random
import sys


log = logging.getLogger(__name__)

class HttpAgent(TrafficClientAgent):
    """
		The wget http generator controls a set of wget clients that make HTTP requests to a set HTTP servers.
		Also look at TrafficClientAgent
	"""
    def __init__(self):
        TrafficClientAgent.__init__(self)

        # Can be support distribution function (look magi.util.distributions)
        self.sizes = '1000'
        self.url = "http://%s/getsize.py?length=%d"

        # SOCKS support
        self.useSocks = False
        self.socksServer = "localhost"
        self.socksPort = 5010
        self.socksVersion = 4

    def getCmd(self, dst):
        cmd = 'curl -o /dev/null -s -S -w data=%{url_effective},%{time_total},%{time_starttransfer},%{size_download},%{speed_download}\\n ' + self.url % (dst, eval(self.sizes))
        if self.useSocks:
            socks_cmd = "--proxy socks%d://%s:%d" % (int(self.socksVersion), self.socksServer, int(self.socksPort))
            cmd = socks_cmd + cmd
        return cmd	
        
    def increaseTraffic(self, msg, stepsize):
        self.sizes = eval(self.sizes) + stepsize
        self.sizes = str(self.sizes)

    def reduceTraffic(self, msg, stepsize):
        self.sizes = eval(self.sizes) - stepsize
        if(self.sizes < 0):
            self.sizes = 0
        self.sizes = str(self.sizes)
               
    def changeTraffic(self, msg, stepsize):
        prob = random.randint(0, 100)
        if prob in range(10):
            self.sizes = eval(self.sizes) + int(stepsize * random.random())
        elif prob in range(10, 20):
            self.sizes = eval(self.sizes) - int(stepsize * random.random())
            if(self.sizes < 0):
                self.sizes = 0
        self.sizes = str(self.sizes)
    
def getAgent(**kwargs):
    agent = HttpAgent()
    agent.setConfiguration(None, **kwargs)
    return agent

if __name__ == "__main__":
    agent = HttpAgent()
    kwargs = initializeProcessAgent(agent, sys.argv)
    agent.setConfiguration(None, **kwargs)
    agent.run()

            