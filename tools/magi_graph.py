#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')
from magi.util import helpers, database, visualization

import logging
import optparse
import sys

if __name__ == '__main__':
 
    optparser = optparse.OptionParser(description="Plots the graph for an experiment based on parameters provided. \
													Experiment Configuration File OR Project and Experiment Name \
                                                    needs to be provided to be able to connect to the experiment.\
                                                    Need to provide build a graph specific configuration for plotting.")
                                                    
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    optparser.add_option("-b", "--base", dest="base", help="Path and name of the experiment config file")
    optparser.add_option("-a", "--agent", dest="agent", help="Agent IDL in the experiment")
    optparser.add_option("-l", "--aal", dest="aal", help="AAL file of the experiment procedure")
    optparser.add_option("-T", "--Tunnel", dest="tunnel", default=False, help="Tunnel request through Deter Ops (users.deterlab.net).")
    optparser.add_option("-u", "--username", dest="username", help="Username for creating tunnel. Required only if different from current shell username.")
    optparser.add_option("-c", "--config", dest="config", help="Path and name of configuration file for generating the graph")
    optparser.add_option("-o", "--output", dest="output", help="Path and name of output file for the graph") 
    (options, args) = optparser.parse_args()
    
    logging.basicConfig(format=helpers.LOG_FORMAT_MSECS, datefmt=helpers.LOG_DATEFMT, level=logging.INFO)
                            
    if options.agent:
        if options.aal:
            logging.info("Attempting to load the Agent IDL file")
            agentidl =  helpers.loadIDL(options.agent,options.aal)
            #logging.info(agentidl['dbfields'])
            logging.info("Displaying field names")
            print
            for field,desc in agentidl['dbfields'].items():
                print field, ':', desc
            print
            logging.info("Loaded IDL file")
        else:
            raise RuntimeError, 'Missing AAL file. Please provide AAL file with option -l'
            sys.exit(2)
    else:                            
     
        if options.config is None:
            optparser.print_help()
            sys.exit(2)
     
        if options.output is None:
            optparser.print_help()
            sys.exit(2)
    
        logging.info("Attempting to get the database config host from the experiment")
        dbConfigNode = helpers.getDBConfigHost(experimentConfigFile=options.base, project=options.project, experiment=options.experiment)
        #logging.info(dbConfigNode)
        logging.info("Got the database config host from the experiment")                                  
    
        logging.info("Attempting to load the Yaml file")
        config = helpers.loadYaml(options.config)
        logging.info("Loaded Yaml file")

        if not config.has_key('db') :
            raise RuntimeError, 'Configuration file incomplete. Database options are missing. Use option -a to get fields'
            sys.exit(2)
        else:
            if not config['db'].has_key('filter'):
                raise RuntimeError, 'Configuration file incomplete. Filter options are missing.Use option -a to get fields'
                sys.exit(2)
        
        #logging.info(config)
   
        try:
            tunnel_cmd = None		
            if options.tunnel:
                logging.info("Attempting to establish SSH tunnel")
                tunnel_cmd = helpers.createSSHTunnel('users.deterlab.net', 27018,
                                                     dbConfigNode, 27017,
                                                     options.username)
                bridge = 'localhost'
                port = 27018
                logging.info('Tunnel setup done')
            else:
                bridge = dbConfigNode
                port = 27017
            
            agentName = config['db']['agent']
            
            try:
                logging.info('Attempting to connect to the database')
                collection = database.getCollection(agentName, bridge, port)
                logging.info('Connected to the database')
            except RuntimeError as e:
                logging.critical("Failed connecting to the database : %s", str(e))
                sys.exit(2)

            x=[]
            y=[]
            logging.info('The filter applied for data collected: %s',config['db']['filter'])	
            for firstvalue in collection.findAll(config['db']['filter']).sort("_id", 1)[:1]:
                logging.info('The first timestamp in database: %s',firstvalue[config['db']['xValue']])

            logging.info('Fetching data from database')
            for post in collection.find(config['db']['filter']).sort("_id", 1):
                x.append("%.8f" % (post[config['db']['xValue']] - firstvalue[config['db']['xValue']]))
                y.append(post[config['db']['yValue']])
          
            logging.info('Constructed the x and y values for graph')
  
            """ Check for type of graph needed and print """          
            
            if config['graph']['type'] == 'line':
                visualization.line_Graph(config['graph']['xLabel'],config['graph']['yLabel'],config['graph']['title'],x,y,options.output)
            elif config['graph']['type'] == 'scatter':
                visualization.scatter_Graph(config['graph']['xLabel'],config['graph']['yLabel'],config['graph']['title'],x,y,options.output)
            logging.info('Printed and saved the graph')	    

        finally:
            if tunnel_cmd:
                logging.info("Closing tunnel")
                helpers.terminateProcess(tunnel_cmd)