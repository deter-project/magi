#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')
import logging
import optparse
import sys

from magi.db.Collection import DB_NAME, COLLECTION_NAME
from magi.util import helpers, visualization
from pymongo import MongoClient


                
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
        else:
            if not options.experimentConfig:
                if not (options.project and options.experiment):
                    optparser.print_help()
                    optparser.error("Missing database host and experiment configuration information")
                    
                options.experimentConfig = helpers.getExperimentConfigFile(
                                            options.project, options.experiment)
                
            from magi.util import config
            # Set the context by loading the experiment configuration file
            config.loadExperimentConfig(options.experimentConfig)
                
            logging.info("Fetching database host based on the experiment information")
            (dbHost, dbPort) = helpers.getExperimentDBHost(
                                experimentConfigFile=options.experimentConfig,
                                project=options.project, 
                                experiment=options.experiment)
            
            logging.info("Fetched database host: %s" %(dbHost)) 
            logging.info("Fetched database port: %s" %(dbPort))    
        
        logging.info("Attempting to load the graph configuration file")
        graphConfig = helpers.loadYaml(options.config)
        logging.info("Graph configuration loaded")

        if not graphConfig.has_key('db') :
            raise RuntimeError, 'Configuration file incomplete. Database options are missing. Use option -a to get fields'
            sys.exit(2)
        
        #logging.info(graphConfig)
        
        dbConfig = graphConfig['db']
        graphConfig = graphConfig.get('graph', {})
   
        try:
            agentName = dbConfig['agent']
            xValue = dbConfig['xValue']
            yValue = dbConfig['yValue']
            dbName = dbConfig.get('dbName', DB_NAME)
            collectionName = dbConfig.get('collectionName', COLLECTION_NAME)
            
            graphType = graphConfig.get('type', 'line')
            xLabel = graphConfig.get('xLabel', xValue)
            yLabel = graphConfig.get('yLabel', yValue)
            graphTitle = graphConfig.get('title', '')
            
        except KeyError:
            raise RuntimeError, 'Configuration file incomplete. Database options are missing. Use option -a to get fields'
            #logging.exception("Invalid graph configuration")
            sys.exit(2)
        
        logging.info("Graph Config: %s", graphConfig)
        logging.info("DB Config: %s", dbConfig)
        
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
                logging.info('Attempting to connect to the database at %s:%d', 
                             dbHost, dbPort)
                logging.info('DB: %s, Collection: %s', dbName, collectionName)
                
                connection = MongoClient(dbHost, dbPort)
                collection = connection[dbName][collectionName]
                
                logging.info('Connected to the database')
            except:
                logging.exception("Failed connecting to the database at %s:%d", 
                                  dbHost, dbPort)
                sys.exit(2)
            
            """ X and Y list values for the graph """
            xValues = []
            yValues = []
            labels = []
            
            if (not dbConfig.has_key('plots')) or (type(dbConfig['plots']) != list):
                raise RuntimeError, "DB Config should have a list of plots"
            
            plots = dbConfig['plots']
                
            logging.info('Number of plots: %d', len(plots))
            plotItr = 0
            
            for plot in plots:
                
                plotItr += 1
                logging.info('Fetching data for plot #%d', plotItr)
                
                if type(plot) != dict:
                    raise RuntimeError, "Each plot entry should be a dictionary"
                
                seriesName = plot.get('series', '')    
                
                dataFilter = plot.get('filter', {})
                dataFilter['agent'] = dbConfig['agent']
                
                logging.info('Data filter: %s', dataFilter)
                
                try:
                    x= []
                    y= []
                    
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
                
                xValues.append(x)
                yValues.append(y)
                labels.append(seriesName)
            
            """ Check for type of graph needed and print """          
            if graphType == 'line':
                visualization.line_Graph(xLabel, yLabel, graphTitle, xValues, yValues, labels, options.output)
            elif graphType == 'scatter':
                visualization.scatter_Graph(xLabel, yLabel, graphTitle, xValues, yValues, labels, options.output)
            
            logging.info('Saved graph at %s' %(options.output))	    

        finally:
            if tunnel_cmd:
                logging.info("Closing tunnel")
                helpers.terminateProcess(tunnel_cmd)
