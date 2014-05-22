#!/usr/bin/env python

from magi.util import querytool
from matplotlib import animation, pyplot as plt, cm
from socket import gaierror
import collections
import logging
import numpy as np
import optparse
import random
import signal
import subprocess
import sys
import time

if __name__ == '__main__':
    optparser = optparse.OptionParser()
    
    optparser.add_option("-b", "--bridge", dest="bridge", help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    optparser.add_option("-T", "--tunnel", dest="tunnel", action="store_true", default=False, help="Tell the tool to tunnel request through Deter Ops (users.deterlab.net).")
    
    (options, args) = optparser.parse_args()
    
    if options.bridge is None:
        optparser.print_help()
        sys.exit(2)
    
    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL ) 
    
    try:    
        tun_proc = None
        try:
            if options.tunnel:
                tun_proc = subprocess.Popen("ssh users.deterlab.net -L 18808:" +
                                            options.bridge + ":18808 -N", shell=True)
                bridge = '127.0.0.1'
                time.sleep(5)
                logging.debug('Tunnel setup done')
            else:
                bridge = options.bridge
        except gaierror as e:
            logging.critical('Error connecting to %s: %s', options.control, str(e))
            exit(3)
            
        
        numNodes = 64
        numProcesses = 100
        
        msgdest = options.bridge.split(".")[0]
        collectionnames = ['processstats']
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

        lasttime = time.time() - 2
            
        def animate(i):
                
            global lasttime
            
            now = time.time()
            timestampChunks = [(now-10, now)]
            lasttime = now
            
            print "timestampChunks............"
            print timestampChunks
            
            data = querytool.getData(collectionnames=collectionnames, 
                                         nodes=nodes, 
                                         filters={},
                                         timestampChunks=timestampChunks, 
                                         bridge=bridge, 
                                         msgdest=msgdest)
        
            expstats = data['processstats']
            
            #print expstats
            
            d = [[0 for x in xrange(ny)] for y in xrange(nx)]
            
            for ix in range(nx):
                nodestats = expstats["node-"+str(ix)]
#                print "node-"+str(ix)
#                print nodestats
                iy=0
                for processstat in nodestats:
                    if iy >= ny:
                        break
                    d[ix][iy] = processstat['cpu_usage']
                    iy += 1
                    
            print d
            
            data = np.array(d)
            cax.set_data(data)
            return cax
                    
        
        anim = animation.FuncAnimation(fig, animate, frames=nx * ny, interval=5000)
        plt.show()
    
    finally:
        if tun_proc:
            tun_proc.terminate()


