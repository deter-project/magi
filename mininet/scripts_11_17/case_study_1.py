#!/usr/bin/python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.cli import CLI
import os
import time
import optparse
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

class MyTopo( Topo ):
    "Simple topology example."

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        Topo.__init__( self )

        # Add hosts and switches
        leftHost = self.addHost( 'h1' )
	directory = "/tmp/h1"
        if not os.path.exists(directory):
            os.makedirs(directory)
            os.makedirs("%s/tmp" % directory)
	
        rightHost = self.addHost( 'h2' )
        directory = "/tmp/h2"
        if not os.path.exists(directory):
            os.makedirs(directory)
            os.makedirs("%s/tmp" % directory)

        # Add links
        self.addLink( leftHost, rightHost )
        
def perfTest(aalfile='cs_procedure.aal'):
	topo = MyTopo()
	net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink)
	net.start()
	log.info("aal file is %s " % aalfile)
	dumpNodeConnections(net.hosts)
	h1, h2 = net.getNodeByName('h1', 'h2')
	log.info( "running daemon on h1")
	#h1.cmd("hostname h1")
	h1.cmd('~/magi/scripts/magi_daemon.py --nodeconf ~/h1.conf -l DEBUG &')
	time.sleep(20)
	log.info("running daemon on h2")
	#h2.cmd("hostname h2")
	h2.cmd('~/magi/scripts/magi_daemon.py --nodeconf ~/h2.conf -l DEBUG &')
	time.sleep(20)	
	log.info("running orchestrator on h1")
	h1.cmd('~/magi/tools/magi_orchestrator.py --events %s -b 10.0.0.1' % aalfile)
	log.info("plotting graph using magi visualtization tool")
	h1.cmd('~/magi/tools/magi_graph.py -d 10.0.0.1 -r 27018 -c graph.conf -o graph.png')
	log.info("graph is stored in graph.png")
	CLI(net)
	log.info( "stopping")
	net.stop()

if __name__ == '__main__':
	optparser = optparse.OptionParser(description="Script to start case study 1")
	optparser.add_option("-a", "--aal", dest="aal", help="Specify location of the aal file, Default: cs_procedure_monitor.aal, ex: -c cs_procedure_monitor.aal ")
	optparser.add_option("-l", "--loglevel", dest="loglevel", help="The level at which to log. Must be one of none, debug, info, warning, error, or critical. Default is info.",default='info', choices=['none', 'all', 'debug', 'info', 'warning', 'error', 'critical'])	
	(options, args) = optparser.parse_args()
	if not options.aal :
		optparser.print_help()
		optparser.error("Missing AAL file")
	log.info("aal file is %s " % options.aal)
	setLogLevel(options.loglevel)
	perfTest(aalfile=options.aal)
