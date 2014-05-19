#!/usr/bin/env python

from magi.util import querytool
from matplotlib import animation, pyplot as plt
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
    optparser.add_option("-t", "--threshold", dest="threshold", type="float", default=50, help="CPU threshold")
    
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
                time.sleep(1)
                logging.debug('Tunnel setup done')
            else:
                bridge = options.bridge
        except gaierror as e:
            logging.critical('Error connecting to %s: %s', options.control, str(e))
            exit(3)
            
        msgdest = options.bridge.split(".")[0]
        collectionnames = ['processstats']
        processInfo = dict()
        
        lasttime = time.time() - 2
        
        while True:
            
            now = time.time()
            timestampChunks = [(lasttime, now)]
            lasttime = now
#            print timestampChunks
            
            data = querytool.getData(collectionnames=collectionnames, 
                                         nodes=None, 
                                         filters={},
                                         timestampChunks=timestampChunks, 
                                         bridge=bridge, 
                                         msgdest=msgdest)
            
            expstats = data['processstats']
            
#            print "expstats............"
#            print timestampChunks
#            print expstats
            
            flag = False
            
            for node in expstats.keys():
                nodestats = expstats[node]
                for processstat in nodestats:
                    if (processstat['cpu_usage'] > options.threshold):
                        flag = True
                        processName = "Not Known"
                        if not node in processInfo:
                            processInfo[node] = querytool.getAgentsProcessInfo(node=node, bridge=bridge, msgdest=msgdest)
                        nodeProcessInfo = processInfo[node]
                        for entry in nodeProcessInfo:
                            if entry['processId'] == processstat['process_id'] and entry.get('threadId', -1) == processstat.get('thread_id', -1):
                                processName = entry['name']
                                break
            
                        print "Node %s, Process Id: %d, Thread Id: %d, Process Name: %s, CPU: %f, Time: %f" % (node, 
                                                                                                               processstat['process_id'], 
                                                                                                               processstat.get('thread_id', 'NA'), 
                                                                                                               processName, 
                                                                                                               processstat['cpu_usage'], 
                                                                                                               processstat['created'])
                        
            if not flag:
                print "All processes are doing fine."
                
            time.sleep(2)

    finally:
        if tun_proc:
            tun_proc.terminate()