#!/usr/bin/env python

from magi.util import querytool
from matplotlib import animation, pyplot as plt
from socket import gaierror
import collections
import logging
import optparse
import os
import signal
import subprocess
import sys
import time

def create_tunnel(server, lport, rhost, rport):
    """
        Create a SSH tunnel and wait for it to be setup before returning.
        Return the SSH command that can be used to terminate the connection.
    """
    ssh_cmd = "ssh %s -L %d:%s:%d -f -o ExitOnForwardFailure=yes -N" % (server, lport, rhost, rport)
    tun_proc = subprocess.Popen(ssh_cmd,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                stdin=subprocess.PIPE)
    while True:
        p = tun_proc.poll()
        if p is not None: break
        time.sleep(1)
    
    if p != 0:
        raise RuntimeError, 'Error creating tunnel: ' + str(p) + ' :: ' + str(tun_proc.stdout.readlines())
    
    return ssh_cmd


if __name__ == '__main__':
    optparser = optparse.OptionParser()
    
    optparser.add_option("-b", "--bridge", dest="bridge", help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    optparser.add_option("-T", "--tunnel", dest="tunnel", action="store_true", default=False, help="Tell the tool to tunnel request through Deter Ops (users.deterlab.net).")
    
    (options, args) = optparser.parse_args()
    
    if options.bridge is None:
        optparser.print_help()
        sys.exit(2)
    
    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL) 
    
    log_format = '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s'
    log_datefmt = '%m-%d %H:%M:%S'
    logging.basicConfig(format=log_format,
                            datefmt=log_datefmt,
                            level=logging.INFO)
    
    try:    
        tunnel_cmd = None
        if options.tunnel:
            tunnel_cmd = create_tunnel('users.deterlab.net', 18808, options.bridge, 18808)
            bridge = '127.0.0.1'
            logging.info('Tunnel setup done')
        else:
            bridge = options.bridge
            
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
            
            logging.info("----------timestampChunks----------")
            logging.info(timestampChunks)
            
            data = querytool.getData(collectionnames=collectionnames, 
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
                if record['port'] == 'out-uc-0':
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
            os.system("kill -9 `ps -ef | grep '" + tunnel_cmd + "' | grep -v grep | awk '{print $2}'`")
