#!/usr/bin/env python

from magi.util import querytool
from socket import gaierror
import logging
import optparse
import signal
import subprocess
import sys
import time
import yaml

def store_list(option, opt_str, value, parser):
    setattr(parser.values, option.dest, value.split(','))

def store_dict(option, opt_str, value, parser):
    setattr(parser.values, option.dest, yaml.load(value))
     
if __name__ == '__main__':
    optparser = optparse.OptionParser()
    
    optparser.add_option("-b", "--bridge", dest="bridge", 
                         help="Address of the bridge node to join the experiment overlay (ex: control.exp.proj)")
    
    optparser.add_option("-T", "--tunnel", dest="tunnel", action="store_true", default=False, 
                         help="Tell the tool to tunnel request through Deter Ops (users.deterlab.net).")
    
    optparser.add_option("-c", "--collections", dest="collections", action="callback", callback=store_list, 
                         default=[], type="string", help="Comma-separated list of the collections to query")
    
    optparser.add_option("-n", "--nodes", dest="nodes", action="callback", callback=store_list, default=[], 
                         type="string", help="Comma-separated list of the nodes to query")
    
    optparser.add_option("-s", "--starttime", dest="starttime", type="float", default = "0", 
                         help="Fetch records created after given start time")
    
    optparser.add_option("-e", "--endtime", dest="endtime", type="float", default = time.time(), 
                         help="Fetch records created before given end time")
    
    optparser.add_option("-f", "--filters", dest="filters", action="callback", callback=store_dict, 
                         default={}, type="string", help="Query filters")
    
    optparser.add_option("-t", "--timeout", dest="timeout", default = "30", 
                         help="Number of seconds to wait to receive the reply")
    
    optparser.add_option("-o", "--logfile", dest="logfile", action='store', 
                         help="If given, log to the file instead of the console (stdout), ex: -o file.log")
    
    optparser.add_option("-l", "--loglevel", dest="loglevel", default='info', 
                         choices=['none', 'all', 'debug', 'info', 'warning', 'error', 'critical'],
                         help="The level at which to log. Must be one of none, debug, info, warning, error, or critical. Default is info.")
    
    (options, args) = optparser.parse_args()
    
    logLevels = {
        'none': 100,
        'all': 0,
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    log_format = '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s'
    log_datefmt = '%m-%d %H:%M:%S'
    
    if options.logfile:
        logging.basicConfig(filename=options.logfile, 
                            filemode='w', 
                            format=log_format,
                            datefmt=log_datefmt,
                            level=logLevels[options.loglevel])
    else:
        logging.basicConfig(format=log_format,
                            datefmt=log_datefmt,
                            level=logLevels[options.loglevel])
    
    # Terminate if the user presses ctrl+c 
    signal.signal(signal.SIGINT, signal.SIG_DFL )
    
    if options.bridge is None or not options.collections:
        optparser.print_help()
        sys.exit(2)
        
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
        
    collectionnames = options.collections
    nodes = options.nodes
    timestampChunks = [(options.starttime, options.endtime)]
    msgdest = options.bridge.split(".")[0]
    filters = options.filters

    data = querytool.getData(collectionnames=collectionnames, 
                             nodes=nodes, 
                             filters=filters,
                             timestampChunks=timestampChunks, 
                             bridge=bridge, 
                             msgdest=msgdest, 
                             timeout=int(options.timeout))
    
    if data:
        logging.info('Data: \n %s ' % ((data)))
    else:
        logging.info('No records found matching the search criteria')
        
    if tun_proc:
        tun_proc.terminate()
        
    exit(0)