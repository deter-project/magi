#!/usr/bin/python

import logging
import logging.handlers
from optparse import OptionParser
import os
import sys
import time

from helpers import call, is_os_64bit, is_running, verifyPythonDevel
from helpers import isInstalled, installPython, installPackage, installPreBuilt

class BootstrapException(Exception): pass
class TarException(BootstrapException): pass
class PBuildException(BootstrapException): pass
class CBuildException(BootstrapException): pass
class PackageInstallException(BootstrapException): pass
class DException(BootstrapException): pass

log = logging.getLogger()

if __name__ == '__main__':
	
	usage = "usage: sudo python networkx_parser.py -f <topology filename>"
	parser = OptionParser()
	parser.add_option("-f", "--file", dest="file", help="json topology FILE")
	parser.add_option("-p", "--distpath", dest="rpath", default="/share/magi/current", help="Location of the distribution") 
	parser.add_option("-U", "--noupdate", dest="noupdate", action="store_true", default=False, help="Do not update the system")
	parser.add_option("-N", "--noinstall", dest="noinstall", action="store_true", default=False, help="Do not install supporting libraries") 
	parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Include debugging information") 
	parser.add_option("-o", "--logfile", dest="logfile", action='store', help="Log file. Default: %default")

	(options, args) = parser.parse_args()
	
	log_format = '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s'
	log_datefmt = '%m-%d %H:%M:%S'
	
	log = logging.getLogger()
	log.handlers = []

	if options.logfile:
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
	
	else:
		handler = logging.StreamHandler()

	handler.setFormatter(logging.Formatter(log_format, log_datefmt))
	log.setLevel(logging.DEBUG)
	log.addHandler(handler)

	log = logging.getLogger(__name__)
	#log.info('set log level to %s (%d)' % (options.loglevel, log.getEffectiveLevel()))

	if not options.file:
		parser.print_help()
		parser.error("Missing topology file")
		
	rpath = options.rpath
	
	try:
	
		if (not options.noupdate) and (not options.noinstall):  # double negative
				if isInstalled('yum'):
						call("yum update")
				elif isInstalled('apt-get'):
						call("apt-get -y update")
				else:
						msg = 'I do not know how to update this system. Platform not supported. Run with --noupdate or on a supported platform (yum or apt-get enabled).'
						log.critical(msg)
						sys.exit(msg)  # write msg and exit with status 1
								
		verifyPythonDevel()
	
		if not options.noinstall:				 
	
			try:
				installPython('PyYAML', 'yaml', 'install', rpath)
			except PBuildException:
				installPython('PyYAML', 'yaml', '--without-libyaml install', rpath)  # try without libyaml if build error
	
			installPython('unittest2', 'unittest2', 'install', rpath)
			installPython('networkx', 'networkx', 'install', rpath)
			
			magidist = 'MAGI-1.6.0'
			installPython(magidist, 'alwaysinstall', 'install', rpath)
			
			installPackage(yum_pkg_name="python-setuptools", apt_pkg_name="python-setuptools")
			installPython('pymongo', 'pymongo', 'install', rpath)
			
			installPackage(yum_pkg_name="mininet", apt_pkg_name="mininet")
			installPackage(yum_pkg_name="xterm", apt_pkg_name="xterm")
			
			if is_os_64bit():
				installPreBuilt('mongodb-linux-x86_64', rpath)
			else:
				installPreBuilt('mongodb-linux-i686', rpath)
			
			#updating sys.path with the installed packages
			import site
			site.main()
			
		# Changing working directory to script's directory
		os.chdir(os.path.dirname(os.path.realpath(__file__)))

		# Now that installation in done on the local node, import utilities 
		from magi import __version__
		from mininet_helpers import createMininetHosts, Mininet, dumpNodeConnections, CLI
		import yaml
		import networkx as nx
		from networkx.readwrite import json_graph
		
		jgraph = yaml.load(open(options.file))
		topoGraph = json_graph.node_link_graph(jgraph)
# 		topoGraph = nx.Graph()
# 		topoGraph.add_nodes_from(['h1','h2'])
# 		topoGraph.add_edge('h1','h2')
# 		print "Nodes %s " % topoGraph.nodes()
# 		print "Edges %s"  % topoGraph.edges()

		log.info("Topology Graph: %s" %(topoGraph))
		
		topo = createMininetHosts(topoGraph)
		net = Mininet(topo=topo, controller=None)
		
		net.start()
		dumpNodeConnections(net.hosts)
	
		#computes shortest path between all pairs of nodes and updates routing table on each node
		for srcNode in topoGraph.nodes():
			log.info("Computing routes for node: %s" %(srcNode))
			for destNode in topoGraph.nodes():
				if srcNode != destNode: 		
					sp = nx.shortest_path(topoGraph, srcNode, destNode)
					log.info("Route from %s to %s is %s" %(srcNode, destNode, sp))
					print net[srcNode].cmd('route add -host %s gw %s-%s' %(destNode, sp[1], srcNode))
			log.info("Routing table for node: %s" %(srcNode))
			log.info(net[destNode].cmd('route'))
			
		hosts = net.hosts
		for node in hosts:
			print node.name, node.IP()	
		
		print topoGraph.nodes()
		
		for nodeName in topoGraph.nodes():
			node = net.getNodeByName(nodeName)
			
			log.info("Starting daemon on %s" %(nodeName))
			
			daemonCmd = '/usr/local/bin/magi_daemon.py --nodeconf /tmp/%s/%s.conf' %(nodeName, nodeName)

			if options.verbose:
				log.info("Starting daemon with debugging")
				daemonCmd += ' -l DEBUG'

			daemonCmd += " &"
			
			log.info(daemonCmd)
			
			log.info("MAGI Version: %s", __version__) 

			node.cmd(daemonCmd)
			
			time.sleep(1)
			
		log.info("Started all daemons and loaded configs")
		
		#net.pingAll()
		CLI(net)
		
		#TODO: Figure out a good way to terminate magi_daemon processes
		os.system("kill `ps -ef | grep magi_daemon | grep -v grep | awk '{print $2}'`")
		time.sleep(2)
		os.system("kill -9 `ps -ef | grep magi_daemon | grep -v grep | awk '{print $2}'`")
		
		net.stop()	
		
	except Exception, e:
		log.exception("Exception while bootstraping")
		sys.exit(e)


