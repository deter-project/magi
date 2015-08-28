#!/usr/bin/python

import logging
import logging.handlers
from optparse import OptionParser
import os
from subprocess import PIPE, Popen
import sys
import time

from magi import __version__
from magi.util import helpers
from magi.util import config

import networkx as nx
from networkx.readwrite import json_graph
from magi.util.database import isDBEnabled

class BootstrapException(Exception): pass

log = logging.getLogger()

def createConfigFiles(nodeName, topoGraph, externalAgentsCommPort=28809, isDBEnabled=True):
	
	config.setNodeDir("/tmp/%s" %(nodeName))
	bridgeNode = helpers.getServer(topoGraph.nodes())
	
	expdl = dict()
	expdl['topoGraph'] = json_graph.node_link_data(topoGraph)
	
	dbdl = dict()
	if isDBEnabled:
		dbdl['isDBEnabled'] = True
		dbdl['isDBSharded'] = False
		dbdl['sensorToCollectorMap'] = {'__ALL__': bridgeNode}
	else:
		dbdl['isDBEnabled'] = False
		
	expConf = dict()
	expConf['expdl'] = expdl
	expConf['dbdl'] = dbdl
	
	expConf = config.loadExperimentConfig(experimentConfig=expConf, 
										  distributionPath='/tmp/magi',
										  transportClass=helpers.TRANSPORT_TCP)
	
	localInfo = dict()
	localInfo['nodename'] = nodeName
	localInfo['processAgentsCommPort'] = externalAgentsCommPort
	
	nodeConf = dict()
	nodeConf['localInfo'] = localInfo
	
	nodeConf = config.loadNodeConfig(nodeConf, expConf)
	
	helpers.makeDir(config.getConfigDir())
	helpers.writeYaml(expConf, config.getExperimentConfFile())
	helpers.writeYaml(nodeConf, config.getNodeConfFile())
	
	return (config.getNodeConfFile(), config.getExperimentConfFile())
	
ETC_HOSTS_BEGIN = "#--MAGI DESKTOP CONFIGURATION BEGIN--#"
ETC_HOSTS_END = "#--MAGI DESKTOP CONFIGURATION END--#"

def clearEtcHostsMininetConfig():
	f = open("/etc/hosts", "r")
	lines = f.readlines()
	f.close()

	f = open("/etc/hosts", "w")
	skip = False
	for line in lines:
		if not skip:
			if line.rstrip() == ETC_HOSTS_BEGIN:
				skip = True
				continue
			f.write(line)

		elif line.rstrip() == ETC_HOSTS_END:
			skip = False

	f.close()
	
def addEtcHostsMininetConfig(configEntries):
	clearEtcHostsMininetConfig()
	f = open("/etc/hosts", "a")
	f.write("%s\n"%(ETC_HOSTS_BEGIN))
	for entry in configEntries:
		f.write(entry)
		f.write("\n")
	f.write("%s\n"%(ETC_HOSTS_END))
	f.close()
	
def store_list(option, opt_str, value, parser):
	setattr(parser.values, option.dest, value.split(','))

if __name__ == '__main__':
	
	optparser = OptionParser()
	optparser.add_option("-n", "--nodes", dest="nodes", action="callback", 
						callback=store_list, default=[], type="string", 
						help="Comma-separated list of the node names")
	#optparser.add_option("-f", "--file", dest="file", help="json topology FILE")
	optparser.add_option("-o", "--logfile", dest="logfile", action='store', 
						default='/tmp/magi_bootstrap.log',  
                        help="Log to specified file, ex: -f file.log. Default: %default")
	optparser.add_option("-l", "--loglevel", dest="loglevel", default="INFO", 
						help="set logger to level ALL, DEBUG, INFO, " + 
                        "WARNING, ERROR. Default: %default, ex: -l DEBUG")
	optparser.add_option("-D", "--nodataman", dest="nodataman", 
						action="store_true", default=False, 
						help="Do not install and setup data manager") 
        

	(options, args) = optparser.parse_args()

	helpers.makeDir(os.path.dirname(options.logfile))
	
	tempDir = '/tmp'
	helpers.makeDir(tempDir)
            
    # Roll over the old log and create a new one
    # Note here that we will have at most 5 logs 
    # Need to check existence of file before creating the handler instance
    # This is because handler creation creates the file if not existent 
	if os.path.isfile(options.logfile):
	    needroll = True
	else:
	    needroll = False
	handler = logging.handlers.RotatingFileHandler(options.logfile, backupCount=5)
	if needroll:
	    handler.doRollover()
	
	handler.setFormatter(logging.Formatter(helpers.LOG_FORMAT_MSECS, helpers.LOG_DATEFMT))
	log = logging.getLogger()
	log.setLevel(helpers.logLevels.get(options.loglevel.upper(), logging.INFO))
	log.handlers = []
	log.addHandler(handler)
	
	if not options.nodes:
		optparser.print_help()
		optparser.error("Missing node names")
        
	try:
		nodeSet = helpers.toSet(options.nodes)
		bridgeNode = helpers.getServer(nodeSet)
		externalAgentsCommPort = 18810
		hostsConfigEntries = []
		topoGraph = nx.Graph()
		
		for nodeName in nodeSet:
			hostsConfigEntries.append("%s	%s" %('127.0.0.1', nodeName))
			topoGraph.add_node(nodeName)
			
		addEtcHostsMininetConfig(hostsConfigEntries)
		
		for nodeName in nodeSet:
			
			(nodeConf, expConf) = createConfigFiles(nodeName=nodeName, 
												    topoGraph=topoGraph,
												    externalAgentsCommPort=externalAgentsCommPort,
												    isDBEnabled=(not options.nodataman))
			externalAgentsCommPort += 1
			
			log.info("Starting daemon for node '%s'" %(nodeName))
			daemonCmd = '/usr/local/bin/magi_daemon.py'
			daemonCmd += ' --expconf %s' %(expConf)
			daemonCmd += ' --nodeconf %s' %(nodeConf)
			daemonCmd += ' -l %s' %(options.loglevel)
			        
			log.info(daemonCmd)
			
			p = Popen(daemonCmd.split(), stdout=PIPE, stderr=PIPE)
			
			import time 
			time.sleep(1)
			
			log.info("MAGI Version: %s", __version__) 
			log.info("Started daemon with pid %s", p.pid)
			
	except Exception, e:
		log.exception("Exception while bootstraping")
		sys.exit(e)


