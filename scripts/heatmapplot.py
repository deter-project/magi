#!/usr/bin/env python

from magi.util import helpers, querytool
from matplotlib import animation, pyplot as plt, cm
import logging
import numpy as np
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
        
        numNodes = 100
        numProcesses = 100
        
        msgdest = options.bridge.split(".")[0]
        agents = ['processstats']
        nodes = ""
        for i in range(numNodes): nodes += ("node-%d, "%i)
        processInfo = dict()
        
        nx = numNodes
        ny = numProcesses
        
        fig, ax = plt.subplots()
        ax.set_xlabel("Agents")
        ax.set_xlim(1, numProcesses)
        ax.set_ylabel("Nodes")
        ax.set_ylim(1, numNodes)
        data = np.zeros((nx, ny))
        cax = ax.imshow(data, interpolation='nearest', cmap=cm.coolwarm, vmin=0, vmax=0.1)
#        ax.set_title('Heatmap')
        
        cbar = fig.colorbar(cax, ticks=[0, 0.05, 0.1], orientation='horizontal')
        cbar.ax.set_xticklabels(['Low', 'Medium', 'High'])# horizontal colorbar

        d = [[0 for x in xrange(ny)] for y in xrange(nx)]
        
        def animate(i):
                
            now = time.time()
            timestampChunks = [(now-10, now)]
            
            logging.info("----------timestampChunks----------")
            logging.info(timestampChunks)
            
            data = querytool.getData(agents=agents, 
                                         nodes=nodes, 
                                         filters={},
                                         timestampChunks=timestampChunks, 
                                         bridge=bridge, 
                                         msgdest=msgdest)
        
            expstats = data['processstats']
            
            #logging.info("----------records----------")
            #logging.info(expstats)
            
            for ix in range(nx):
                nodestats = expstats["node-"+str(ix)]
                #logging.info("node-"+str(ix))
                #logging.info(nodestats)
                iy=0
                for processstat in nodestats:
                    if iy >= ny:
                        break
                    d[ix][iy] = processstat['cpu_usage']
                    iy += 1
                    
            logging.info("----------data----------")
            logging.info(d)
            
            data = np.array(d)
            cax.set_data(data)
            return cax

                
        anim = animation.FuncAnimation(fig, animate, frames=nx * ny, interval=5000)
        plt.show()
    
    finally:
        if tunnel_cmd:
            logging.info("Closing tunnel")
            helpers.terminateProcess(tunnel_cmd)
