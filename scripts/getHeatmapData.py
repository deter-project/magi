#!/usr/bin/env python

from magi.util import querytool
import logging
import optparse
import os
import subprocess
import sys
import time

from pymongo import MongoClient

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
        
        numNodes = 100
        numProcesses = 100
        
        msgdest = options.bridge.split(".")[0]
        collectionnames = ['processstats']
        nodes = ""
        for i in range(numNodes): nodes += ("node-%d, "%i)
        processInfo = dict()
        
        nx = numNodes
        ny = numProcesses

        d = [[0 for x in xrange(ny)] for y in xrange(nx)]
        
        mongo = MongoClient()
        
        while True:
                
            now = time.time()
            timestampChunks = [(now-10, now)]
            
            logging.info("----------timestampChunks----------")
            logging.info(timestampChunks)
            
            data = querytool.getData(collectionnames=collectionnames, 
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
            
            highChartsData = []
            for ix in range(nx):
                for iy in range(ny):
                    highChartsData.append([ix, iy, d[ix][iy]])
            
            logging.info("----------highChartsData----------")
            logging.info(highChartsData)
            
            logging.info("Inserting into DB")
            mongo['magi']['heatmap'].insert({'created': time.time(), 'data': highChartsData})
            mongo['magi']['heatmap'].remove({'created': {'$lt': time.time()-10}})
            logging.info("DB updation done")
            
            
                         
    finally:
        if tunnel_cmd:
            logging.info("Closing tunnel")
            os.system("kill -9 `ps -ef | grep '" + tunnel_cmd + "' | grep -v grep | awk '{print $2}'`")
