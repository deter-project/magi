#!/usr/bin/env python

import yaml
import logging
import optparse
import matplotlib
matplotlib.use('Agg')
from pymongo import MongoClient
import matplotlib.pyplot as plt
import sys
import os,stat
import subprocess
import time



def create_tunnel(username, server, lport, rhost, rport):
    """
        Create a SSH tunnel and wait for it to be setup before returning.
        Return the SSH command that can be used to terminate the connection.
    """
    ssh_cmd = "ssh %s@%s -L %d:%s:%d -f -o ExitOnForwardFailure=yes -N" % (username,server, lport, rhost, rport)
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

def load_yaml(file_aal):
   try:
		f=open(file_aal)  
		config=yaml.load(f)
        	return config
   except IOError as e:
		logging.critical("File not found: %s", str(e))
		sys.exit(2)
	 	 

if __name__ == '__main__':
 
   optparser = optparse.OptionParser()
   optparser.add_option("-T", "--tunnel", dest="tunnel", default=False, help="Tunnel request through Deter Ops (users.deterlab.net). Specify username")
   optparser.add_option("-c", "--config", dest="config", help="Path and name of configuration file for generating the graph")
   optparser.add_option("-o", "--output", dest="output", help="Path and name of output file for the graph") 
   
   (options, args) = optparser.parse_args()
     
   if options.config is None:
    optparser.print_help()
    sys.exit(2)
     
   if options.output is None:
    optparser.print_help()
    sys.exit(2)
      
   log_format = '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s'
   log_datefmt = '%m-%d %H:%M:%S'
   logging.basicConfig(format=log_format,
                            datefmt=log_datefmt,
                            level=logging.INFO)

   logging.info("Attempting to load the Yaml file")
   config = load_yaml(options.config)
   logging.info("Loaded Yaml. Length of file: %s", len(config))
   #logging.info(config)
   
   
   try:
     tunnel_cmd = None		
     if options.tunnel:
 	   logging.info("Attempting to establish SSH tunnel")
	   tunnel_cmd = create_tunnel(options.tunnel,'users.deterlab.net',27018,config['base']['nid'] + "." + config['base']['eid'] + "." + config['base']['pid'], 27017)
           bridge = 'localhost'
	   port = 27018
           logging.info('Tunnel setup done')
     else:
           bridge = config['base']['nid'] + "." + config['base']['eid'] + "." + config['base']['pid']
           port = 27017
	   #print bridge

     logging.info('Attempting to connect to the database')
     try:
  	  connection = MongoClient(bridge,port)
     except RuntimeError as e:
   	  logging.critical("Failed connecting to the database : %s", str(e))
   	  sys.exit(2)

     logging.info('Connected to the database')
     db = connection['magi']
     collection = db[config['db']['cid']]
   	 
     x=[]
     y=[]
     for firstvalue in collection.find(config['db']['filter']).sort("_id",1)[:1]:
        logging.info('The first timestamp in database: %s',firstvalue[config['db']['xValue']])

     logging.info('Fetching data from database')
     for post in collection.find(config['db']['filter']).sort("_id",1):
	     #logging.info(firstvalue[config['db']['xValue']])
	     #logging.info(post[config['db']['xValue']])
      	     x.append("%.8f" % (post[config['db']['xValue']] - firstvalue[config['db']['xValue']]))
      	     y.append("%.3f" % ((post[config['db']['yValue']] * 8)/1000000.0))
      	     #logging.info((post[config['db']['yValue']] * 8)/1000000.0)
      	     #y.append(post[config['db']['yValue']])
          
     logging.info('Constructed the x and y values for graph')
     #logging.info(x)
     #logging.info(y)
 
     logging.info('Preparing to plot values for graph')
     plt.xlabel(config['graph']['xLabel'])
     plt.ylabel(config['graph']['yLabel'])	
     plt.title(config['graph']['title'])
     ax = plt.subplot(111)
     ax.spines["right"].set_visible(False)
     ax.spines["top"].set_visible(False)
     ax.xaxis.set_ticks_position('bottom')
     ax.yaxis.set_ticks_position('left')
     lines = plt.plot(x, y)
     plt.setp(lines, 'color', 'r', 'linewidth', 2.0)
     plt.savefig(options.output)
     logging.info('Printed and saved the graph')	    
 
   finally:
     if tunnel_cmd:
          logging.info("Closing tunnel")
          os.system("kill -9 `ps -ef | grep '" + tunnel_cmd + "' | grep -v grep | awk '{print $2}'`")
