#!/usr/bin/env python

from magi.util import querytool
from matplotlib import animation, pyplot as plt
import collections
import subprocess
import time
import optparse
import sys
import logging
import signal
from socket import gaierror

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
            
        msgdest = options.bridge.split(".")[0]
        
        collectionnames = ['pktcounter']
        nodes = ['rc']
        filters = {'port': { '$in': ['out-uc-0', 'out-c-0'] } }
                       
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
            
            print "timestampChunks............"
            print timestampChunks
            
            data = querytool.getData(collectionnames=collectionnames, 
                                         nodes=nodes, 
                                         filters=filters,
                                         timestampChunks=timestampChunks, 
                                         bridge=bridge, 
                                         msgdest=msgdest)
        
            records = data['pktcounter']['rc']
            
            print "records............"
            print records
        
            for record in records:
                #print record['created'], record['port'], record['bytes']
                created = int(record['created']) - starttime
                if record['port'] == 'out-uc-0':
                    uc[created] = record['bytes']
                else:
                    c[created] = record['bytes']
                    
                t[created] = t.get(created, 0) + record['bytes']
        
            uc = collections.OrderedDict(sorted(uc.items()))
            c = collections.OrderedDict(sorted(c.items()))
            t = collections.OrderedDict(sorted(t.items()))
        
#            print "data............"
#            print uc
#            print c
#            print t
        
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
        if tun_proc:
            tun_proc.terminate()


