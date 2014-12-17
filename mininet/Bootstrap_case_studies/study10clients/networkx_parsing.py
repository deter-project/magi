#!/usr/bin/python

from networkx import *
from collections import deque
import networkx as nx
import os
from mininet.topo import Topo, MultiGraph
from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import CPULimitedHost, Node
from mininet.link import Link
from mininet.util import dumpNodeConnections
from mininet.cli import CLI
from mininet.util import irange
from collections import defaultdict
from optparse import OptionParser
import time
import yaml
from networkx.readwrite import json_graph


iplist = deque([])

for i in range (20):
	ip = '10.0.%s.1/24'%i
	iplist.append(ip)
	ip = '10.0.%s.2/24'%i
	iplist.append(ip)



G=nx.Graph()

ETC_HOSTS_BEGIN = "# MININET CONFIGURATION BEGIN"
ETC_HOSTS_END = "# MININET CONFIGURATION END"


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


'''
enables forwarding in nodes
'''
class LinuxRouter( Node ):
    "A Node with IP forwarding enabled."

    def config( self, **params ):
        super( LinuxRouter, self).config( **params )
        # Enable forwarding on the router
        self.cmd( 'sysctl net.ipv4.ip_forward=1' )

    def terminate( self ):
        self.cmd( 'sysctl net.ipv4.ip_forward=0' )
        super( LinuxRouter, self ).terminate()

class create_mininet_hosts(Topo):
	
    	"Simple topology example."

    	def __init__( self ):
        	Topo.__init__( self, inNamespace=False )
		interfacelist = defaultdict(list)

#Creates mininet hosts that can route packets and generates list of interfaces necessary for each host

		for v in G.nodes():
			#righthost = self.addNode('%s'%v, '''ip=iplist.popleft(),''' cls=LinuxRouter)
			righthost = self.addNode('%s'%v, cls=LinuxRouter, ip='')
			neighbors = G.neighbors(v)
			intflist = deque([])
			for i in range(len(neighbors)):
				intfname = '%s-eth%s'%(v, i)				
				intflist.append(intfname)
			interfacelist[v].append(intflist)
			
# create config files for nodes
			directory = "/tmp/%s" % v
			if not os.path.exists(directory):
				os.makedirs(directory)
				os.makedirs("%s/tmp" % directory)
			client = '%s' % v
			if client != 'server':
				confFile = "/tmp/" + client +"/" + client +".conf"
				with open(confFile, 'w') as conf:
					conf.write("localInfo:\n")
					conf.write("  configDir: /tmp/"+client+"/config/\n")
					conf.write("  logDir: /tmp/"+client+"/logs/\n")
					conf.write("  nodename: "+client+"\n")
					conf.write("  dbDir: /tmp/"+client+"/db\n")
					conf.write("  tempDir: /tmp/"+client+"/tmp\n")
					conf.write("database:\n")
					conf.write("  isDBEnabled: true\n")
					conf.write("  isDBSharded: false\n")
					conf.write("  sensorToCollectorMap: {"+client+": server}\n")
					conf.write("  configHost: server1\n")
					conf.write("transports:\n- {address: server, class: TCPTransport, port: 28808}\nprocessAgentsCommPort: 18810")
				conf.close()
			elif client == 'server':
				server = client
				confFile = "/tmp/" + server +"/"+server+".conf"
				with open(confFile, 'w') as conf:
					conf.write("localInfo:\n")
					conf.write("  configDir: /tmp/"+server+"/config/\n")
					conf.write("  logDir: /tmp/"+server+"/logs/\n")
					conf.write("  nodename: "+server+"\n")
					conf.write("  dbDir: /tmp/"+server+"/db\n")
					conf.write("  tempDir: /tmp/"+server+"/tmp\n")
					conf.write("database:\n")
					conf.write("  isDBEnabled: true\n")
					conf.write("  isDBSharded: false\n")
					conf.write("  sensorToCollectorMap: {"+server+": server}\n")
					
					conf.write("software:\n")
					conf.write("- {dir: /home/seth, type: source}\n")
					conf.write("- {type: apt}\n")
					#conf.write("transports:\n- {address: server, class: TCPTransport, port: 28808}\nprocessAgentsCommPort: 18810")
				conf.close()
			else:
				continue 

		print "final %s" % interfacelist.items() 

#adds edges in mininet topology and configures Ip addresses
		
		clearEtcHostsMininetConfig()
		hostfile = open ("/etc/hosts", "a")
		
		hostfile.write(ETC_HOSTS_BEGIN + "\n")
		for e in G.edges():
			#node1, node2 = self.get(e[0]), self.get(e[1]) 
			a = []
			a.append(e[0])
			a.append(e[1])
			a.sort()
			print "adding link between %s and %s" %(a[0], a[1])
			intf1name = interfacelist[a[0]]
			intf2name = interfacelist[a[1]]			
			print "intf1 name = %s,intf1 name = %s" %(intf1name[0], intf2name[0])
			interface1 = intf1name[0].popleft()
			interface2 = intf2name[0].popleft()
			
			interfacelist[a[0]]=[intf1name[0]]
			interfacelist[a[1]]=[intf2name[0]]
			
			ip1 = iplist.popleft()
			ip2 = iplist.popleft()
			
			self.addLink( a[0], a[1], intfName1=interface1, intfName2=interface2, params1={'ip': ip1 }, params2={ 'ip' : ip2 } )
			hostfile.write("%s	%s-%s %s\n" %(ip1.split('/')[0], a[0], a[1], a[0]))
			hostfile.write("%s	%s-%s %s\n" %(ip2.split('/')[0], a[1], a[0], a[1]))
			print "\n\nadded link between %s and %s\n\n" %(a[0], a[1])
			
		hostfile.write(ETC_HOSTS_END +"\n")			
		hostfile.close()

				

if __name__ == '__main__':
	
	
	usage = "usage: sudo python networkx_parser.py -f <topology filename>"
	parser = OptionParser()
	parser.add_option("-f", "--file", dest="file", help="json topology FILE")
	
	(options, args) = parser.parse_args()
	
	if options.file:
		jgraph = yaml.load(open(options.file))
		G = json_graph.node_link_graph(jgraph)
	else:
            	parser.print_help()
            	parser.error("Missing topology file")
	topo = create_mininet_hosts()
	net = Mininet(topo=topo, controller=None)
	net.start()
	dumpNodeConnections(net.hosts)

#computes shortest path between all pairs of nodes and updates routing table on each node
	
	
	for v in G.nodes():
		for u in G.nodes():
			if u != v: 		
				sp = shortest_path(G, v, u)
				print "route from = %s, routes to = %s is %s "%( v, u, sp)
				print net[v].cmd('route add -host %s gw %s-%s' %(u, sp[1], v))
		#print net[v].cmd('route')
	for v in G.nodes():
		h1 = net.getNodeByName( '%s' %v )
	
		print "running daemon on %s" % v
		h1.cmd('~/magi/scripts/magi_daemon.py --nodeconf /tmp/%s/%s.conf -l DEBUG &' %(v, v))
		time.sleep(1)
	print "started all daemons and locaded configs"
	#net.pingAll()
	CLI(net)
	net.stop()	


