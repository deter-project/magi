#!/usr/bin/env python

from magi.util import helpers, visualization
from pymongo import MongoClient
import logging
import matplotlib
matplotlib.use('Agg')
import optparse
import sys

#cannot import from magi.util.database as it needs testbed specific information
#that might not be available on all nodes from where magi_graph tool is run
#from magi.util.database import DB_NAME, COLLECTION_NAME
DB_NAME = 'magi'
COLLECTION_NAME = 'experiment_data'
                
if __name__ == '__main__':
 
    optparser = optparse.OptionParser(description="Plots the graph for an experiment based on parameters provided. \
													Experiment Configuration File OR Project and Experiment Name \
                                                    needs to be provided to be able to connect to the experiment.\
                                                    Need to provide build a graph specific configuration for plotting.")
                                                    
    optparser.add_option("-d", "--dbhost", dest="dbhost", help="Database host")
    optparser.add_option("-r", "--dbport", dest="dbport", type="int", default=27017, help="Database port")
    optparser.add_option("-x", "--experimentConfig", dest="experimentConfig", help="Experiment configuration file")
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
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
            helpers.printDBfields(agentidl)
            logging.info("Loaded IDL file")
        else:
            optparser.print_help()
            optparser.error("Missing AAL file")
    else:                            
        if options.config is None:
            optparser.print_help()
            optparser.error("Missing configuration file")
     
        if options.dbhost:
            dbHost = options.dbhost
            dbPort = options.dbport
        elif options.experimentConfig or (options.project and options.experiment):
            logging.info("Fetching database config host based on the experiment information")
            (dbHost, dbPort) = helpers.getDBConfigHost(experimentConfigFile=options.experimentConfig,
                                                       project=options.project, 
                                                       experiment=options.experiment)
            logging.info("Fetched database config host: %s" %(dbHost))    
        else:
            optparser.print_help()
            optparser.error("Missing database host and experiment configuration information")
        
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
            dataFilter['agent'] = agentName
            xValue = graphConfig.get('xValue','created')
            yValue = dbConfig['yValue']
            graphType = graphConfig.get('type', 'line')
            xLabel = graphConfig.get('xLabel', xValue)
            yLabel = graphConfig.get('yLabel', yValue)
            graphTitle = graphConfig.get('title', 'Graph')
        except KeyError:
            raise RuntimeError, 'Configuration file incomplete. Database options are missing. Use option -a to get fields'
            #logging.exception("Invalid graph configuration")
            sys.exit(2)
            
        try:
            tunnel_cmd = None		
            if options.tunnel:
                logging.info("Attempting to establish SSH tunnel")
                tunnel_cmd = helpers.createSSHTunnel('users.deterlab.net', dbPort,
                                                     dbHost, dbPort,
                                                     options.username)
                dbHost = 'localhost'
                logging.info('Tunnel setup done')
            
            try:
                logging.info('Attempting to connect to the database')
                connection = MongoClient(dbHost, dbPort)
                collection = connection[DB_NAME][COLLECTION_NAME]
                logging.info('Connected to the database')
            except:
                logging.exception("Failed connecting to the database %s:%s" %(dbHost, str(dbPort)))
                sys.exit(2)

            """ X and Y list values for the graph """
            x=[]
            y=[]
            logging.info('The filter applied for data collected: %s', dataFilter)
            
            try:
                """ Populating the X and Y values from the database """
                firstRecord = collection.find(dataFilter).sort(xValue, 1).limit(1)[0]
                logging.info('The first timestamp in database: %s', firstRecord[xValue])
    
                logging.info('Fetching data from database')
                for record in collection.find(dataFilter).sort(xValue, 1):
                    x.append("%.8f" % (record[xValue] - firstRecord[xValue]))
                    y.append(record[yValue])
            except:
                logging.exception("Error fetching data")
                sys.exit(2)
                
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
