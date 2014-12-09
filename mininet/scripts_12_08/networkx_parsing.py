#!/usr/bin/python

from networkx import *
from collections import deque
import networkx as nx

from mininet.topo import Topo, MultiGraph
from mininet.net import Mininet
from mininet.log import setLogLevel
from mininet.node import CPULimitedHost, Node
from mininet.link import Link
from mininet.util import dumpNodeConnections
from mininet.cli import CLI
from mininet.util import irange
from collections import defaultdict

iplist = deque([])

for i in range (20):
	ip = '10.0.%s.1/24'%i
	iplist.append(ip)
	ip = '10.0.%s.2/24'%i
	iplist.append(ip)



G=nx.Graph()
G.add_nodes_from(['h1','h2','h3','h4'])

G.add_edge('h1','h2')
G.add_edge('h1','h3')
G.add_edge('h1','h4')	
#print "Nodes %s " % G.nodes()
#print "Edges %s"  % G.edges()

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
		print "final %s" % interfacelist.items() 

#adds edges in mininet topology and configures Ip addresses
		
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
			#print "popped intf1 name = %s,intf1 name = %s" %(interface1, interface2)
			#print "after poping intf1 name = %s,intf2 name = %s" %(intf1name[0], intf2name[0])
			interfacelist[a[0]]=[intf1name[0]]
			interfacelist[a[1]]=[intf2name[0]]
			#print "final %s" % interfacelist.items()
			#print "before IPs %s %s"%(node1.IP(), node1.IP())
			#e[0].setIP(iplist.popleft())
			#e[1].setIP(iplist.poplefft())
			#print "after IPs %s %s"%(e[0].IP(), e[1].IP())
			#link1 = Link.__init__(e[0], e[1], intfName1=interface1, intfName2=interface2, addr1=iplist.popleft(), addr2=iplist.popleft())
			self.addLink( a[0], a[1], intfName1=interface1, intfName2=interface2, params1={'ip': iplist.popleft() }, params2={ 'ip' : iplist.popleft() } )

			print "\n\nadded link between %s and %s\n\n" %(a[0], a[1])
			#print self.linkInfo(a[0], a[1])			


				

if __name__ == '__main__':
	
	
	#print iplist
	
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
				print net[v].cmd('route add -host %s gw %s' %(u, sp[1]))
		print net[v].cmd('route')
	hosts = net.hosts
	for node in hosts:
		print node.name, node.IP()	
	'''	
	hosts = net.hosts
	for node in hosts:
		for dest in hosts:
			if node != dest:
				sp = shortest_path(G, node.name, dest.name)
				if len(sp) > 2: 
					print "route from = %s, routes to = %s is %s "%( node.name, dest.name, sp)
					print node.cmd('route add -host %s gw %s' %(dest.IP(), sp[1]))
		print node.cmd('route')
	'''	
	net.pingAll()
	CLI(net)
	net.stop()	


