#!/usr/bin/env python

from magi.util import helpers, querytool
from matplotlib import animation, pyplot as plt
import collections
import logging
import optparse
import sys
import time

if __name__ == '__main__':
    optparser = optparse.OptionParser()
    
    optparser.add_option("-b", "--bridge", dest="bridge", help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    optparser.add_option("-t", "--tunnel", dest="tunnel", action="store_true", default=False, help="Tell the tool to tunnel request through Deter Ops (users.deterlab.net).")
    optparser.add_option("-u", "--username", dest="username", help="Username for creating tunnel. Required only if different from current shell username.")
    
    (options, args) = optparser.parse_args()
    
    if options.bridge is None:
        optparser.print_help()
        sys.exit(2)
    
    logging.basicConfig(format=helpers.LOG_FORMAT_MSECS, datefmt=helpers.LOG_DATEFMT, level=logging.INFO)
    
    try:    
        tunnel_cmd = None
        if options.tunnel:
            tunnel_cmd = helpers.createSSHTunnel('users.deterlab.net', 18808, options.bridge, 18808, options.username)
            bridge = '127.0.0.1'
            logging.info('Tunnel setup done')
        else:
            bridge = options.bridge
            
        msgdest = options.bridge.split(".")[0]
        
        agents = ['pktcounter']
        nodes = ['rc']
        filters = {'peerNode': { '$in': ['uc-0', 'c-0'] }, 'trafficDirection' : 'out' }
                       
        uc = dict()
        c = dict()
        t = dict()
        
        lasttime = 0
        starttime = time.time()
            
        def update():
                
            global uc, c, t, lasttime
            
            now = time.time()
            timestampChunks = [(lasttime, now)]
            lasttime = now
            
            logging.info("----------timestampChunks----------")
            logging.info(timestampChunks)
            
            data = querytool.getData(agents=agents, 
                                         nodes=nodes, 
                                         filters=filters,
                                         timestampChunks=timestampChunks, 
                                         bridge=bridge, 
                                         msgdest=msgdest)
        
            records = data['pktcounter']['rc']
            
            logging.info("----------records----------")
            logging.info(records)
        
            for record in records:
                #print record['created'], record['port'], record['bytes']
                created = int(record['created']) - starttime
                if record['peerNode'] == 'uc-0':
                    uc[created] = record['bytes']
                else:
                    c[created] = record['bytes']
                    
                t[created] = t.get(created, 0) + record['bytes']
        
            uc = collections.OrderedDict(sorted(uc.items()))
            c = collections.OrderedDict(sorted(c.items()))
            t = collections.OrderedDict(sorted(t.items()))
            
            #logging.info("----------data----------")
            #logging.info(uc)
            #logging.info(c)
            #logging.info(t)
        
        update()
        
        fig = plt.figure()
        plt.xlabel('Time (seconds)')
        plt.ylabel('Traffic (Bytes/second)')
        ax = fig.add_subplot(1,1,1)
        
        start = uc.keys()[0]
        ax.set_xlim(start, start+300)
        ax.set_ylim(0, 15000000)
        ax.axhspan(10000000, 10500000, facecolor='0.5', alpha=0.5)
        
        def animate(i):
            #ax.clear()
            update()
            
            ax.plot(uc.keys(), uc.values(), 'r', label="Noise")
            ax.plot(c.keys(), c.values(), 'g', label="Controlled Traffic")
            ax.plot(t.keys(), t.values(), 'b', label="Total Traffic")
            
        
        ani = animation.FuncAnimation(fig, animate, interval=2000)
        legend = ax.legend(loc='upper right', shadow=True)
        
        plt.show()
    
    finally:
        if tunnel_cmd:
            logging.info("Closing tunnel")
            helpers.terminateProcess(tunnel_cmd)
