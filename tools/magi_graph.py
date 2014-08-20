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
    optparser.add_option("-x", "--experimentConfig", dest="experimentConfig", help="Experiment configuration file")
    optparser.add_option("-c", "--config", dest="config", help="Graph configuration file")
    optparser.add_option("-a", "--agent", dest="agent", help="Agent name. This is used to fetch available database fields")
    optparser.add_option("-l", "--aal", dest="aal", help="AAL (experiment procedure) file. This is also used to fetch available database fields")
    optparser.add_option("-o", "--output", dest="output", default='graph.png', help="Output graph file. Default: %default")
    optparser.add_option("-t", "--tunnel", dest="tunnel", action="store_true", default=False, help="Tell the tool to tunnel request through Deter Ops (users.deterlab.net).")
    optparser.add_option("-u", "--username", dest="username", help="Username for creating tunnel. Required only if different from current shell username.")
    (options, args) = optparser.parse_args()
    
    logging.basicConfig(format=helpers.LOG_FORMAT_MSECS, datefmt=helpers.LOG_DATEFMT, level=logging.INFO)
                            
    if options.agent:
        if options.aal:
            logging.info("Attempting to load the Agent IDL file")
            agentidl =  helpers.loadIDL(options.agent, options.aal)
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
     
        logging.info("Attempting to get the database config host from the experiment")
        dbConfigNode = helpers.getDBConfigHost(experimentConfigFile=options.experimentConfig, 
                                               project=options.project, 
                                               experiment=options.experiment)
        #logging.info(dbConfigNode)
        logging.info("Got the database config host from the experiment")                                  
    
        logging.info("Attempting to load the graph configuration file")
        config = helpers.loadYaml(options.config)
        logging.info("Graph configuration loaded")

        if not config.has_key('db') :
            raise RuntimeError, 'Configuration file incomplete. Database options are missing. Use option -a to get fields'
            sys.exit(2)
        
        #logging.info(config)
        
        dbConfig = config['db']
        graphConfig = config.get('graph', {})
   
        try:
            agentName = dbConfig['agent']
            dataFilter = dbConfig.get('filter', {})
            xValue = dbConfig['xValue']
            yValue = dbConfig['yValue']
            graphType = graphConfig.get('type', 'line')
            xLabel = graphConfig.get('xLabel', xValue)
            yLabel = graphConfig.get('yLabel', yValue)
            graphTitle = graphConfig.get('title', 'Graph')
        except KeyError:
            logging.exception("Invalid graph configuration")
            sys.exit(2)
            
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
            
            try:
                logging.info('Attempting to connect to the database')
                collection = database.getCollection(agentName, bridge, port)
                logging.info('Connected to the database')
            except RuntimeError as e:
                logging.critical("Failed connecting to the database : %s", str(e))
                sys.exit(2)
            
            x=[]
            y=[]
            logging.info('The filter applied for data collected: %s', dataFilter)
            firstRecord = collection.findAll(dataFilter).sort(xValue, 1).limit(1)[0]
            logging.info('The first timestamp in database: %s', firstRecord[xValue])

            logging.info('Fetching data from database')
            for record in collection.findAll(dataFilter).sort(xValue, 1):
                x.append("%.8f" % (record[xValue] - firstRecord[xValue]))
                y.append(record[yValue])
          
            logging.info('Constructed the x and y values for graph')
  
            """ Check for type of graph needed and print """          
            
            if graphType == 'line':
                visualization.line_Graph(xLabel, yLabel, graphTitle, x, y, options.output)
            elif graphType == 'scatter':
                visualization.scatter_Graph(xLabel, yLabel, graphTitle, x, y, options.output)
            logging.info('Saved graph at %s' %(options.output))	    

        finally:
            if tunnel_cmd:
                logging.info("Closing tunnel")
                helpers.terminateProcess(tunnel_cmd)
