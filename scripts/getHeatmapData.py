#!/usr/bin/env python

from magi.util import helpers, querytool
import logging
import optparse
import sys
import time

from pymongo import MongoClient

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

        d = [[0 for x in xrange(ny)] for y in xrange(nx)]
        
        mongo = MongoClient()
        
        while True:
                
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
            helpers.terminateProcess(tunnel_cmd)
