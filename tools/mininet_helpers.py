#!/usr/bin/python

from collections import defaultdict
from collections import deque
import logging
import os

from magi.util import helpers
import yaml

from mininet.cli import CLI
from mininet.net import Mininet
from mininet.node import Node
from mininet.topo import Topo
from mininet.util import dumpNodeConnections


log = logging.getLogger()

iplist = deque([])

for i in range (20):
	ip = '10.0.%s.1/24'%i
	iplist.append(ip)
	ip = '10.0.%s.2/24'%i
	iplist.append(ip)

ETC_HOSTS_BEGIN = "#--MININET CONFIGURATION BEGIN--#"
ETC_HOSTS_END = "#--MININET CONFIGURATION END--#"

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

'''
	Enables forwarding in nodes
'''
class LinuxRouter(Node):
	"A Node with IP forwarding enabled."
	
	def config(self, **params):
		super(LinuxRouter, self).config(**params)
		# Enable forwarding on the router
		self.cmd('sysctl net.ipv4.ip_forward=1')
	
	def terminate(self):
		self.cmd('sysctl net.ipv4.ip_forward=0')
		super(LinuxRouter, self).terminate()

class createMininetHosts(Topo):
	
	def __init__(self, topoGraph):
		Topo.__init__(self)
		interfacelist = defaultdict(list)
		
		nodes = topoGraph.nodes()
		nodes.sort()
		bridgeNode = nodes[0]
		for node in nodes:
			if 'control' == node.lower():
				bridgeNode = 'control'
				break

		#Creates mininet hosts that can route packets and generates list of interfaces necessary for each host
		for nodeName in topoGraph.nodes():
			self.addNode(name=nodeName, cls=LinuxRouter, ip='')
			neighbors = topoGraph.neighbors(nodeName)
			intflist = deque([])
			for i in range(len(neighbors)):
				intfname = '%s-eth%s'%(nodeName, i)				
				intflist.append(intfname)
			interfacelist[nodeName].append(intflist)
			
			# create config files for nodes
			directory = "/tmp/%s" % nodeName
			if not os.path.exists(directory):
				os.makedirs(directory)
				os.makedirs("%s/tmp" % directory)
			
			localInfo = dict()
			localInfo['configDir'] = "/tmp/%s/config" %(nodeName)
			localInfo['logDir'] = "/tmp/%s/logs" %(nodeName)
			localInfo['dbDir'] = "/tmp/%s/db" %(nodeName)
			localInfo['tempDir'] = "/tmp/%s/tmp" %(nodeName)
			localInfo['nodename'] = nodeName
			
			helpers.makeDir(localInfo['configDir'])
			
			dbConfig = dict()
			dbConfig['isDBEnabled'] = True
			dbConfig['isDBSharded'] = False
			dbConfig['sensorToCollectorMap'] = {nodeName: nodeName}
			
			transportsConfig = []
			if nodeName == bridgeNode:
				transportsConfig.append({'class': 'TCPServer', 'address': '0.0.0.0', 'port': 18808})
				transportsConfig.append({'class': 'TCPServer', 'address': '0.0.0.0', 'port': 28808})
			else:
				transportsConfig.append({'class': 'TCPTransport', 'address': bridgeNode, 'port': 28808})
				
			nodeConfig = dict()
			nodeConfig['localInfo'] = localInfo
			nodeConfig['database'] = dbConfig
			nodeConfig['transports'] = transportsConfig
			
			nodeConfFile = "/tmp/%s/%s.conf" %(nodeName, nodeName)
			fp = open(nodeConfFile, 'w')
			fp.write(yaml.safe_dump(nodeConfig))
			fp.close()
			
		print "Interface List: %s" % interfacelist.items() 
			
		#adds edges in mininet topology and configures Ip addresses
		hostsConfigEntries = []
		for e in topoGraph.edges():
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
			#link1 = Link.__init__(e[0], e[1], intfName1=interface1, intfName2=interface2, addr1=iplist.popleft(), addr2=iplist.popleft())
			self.addLink( a[0], a[1], intfName1=interface1, intfName2=interface2, params1={'ip': ip1 }, params2={ 'ip' : ip2 } )
			hostsConfigEntries.append("%s	%s-%s %s" %(ip1.split('/')[0], a[0], a[1], a[0]))
			hostsConfigEntries.append("%s	%s-%s %s" %(ip2.split('/')[0], a[1], a[0], a[1]))
			print "\n\nadded link between %s and %s\n\n" %(a[0], a[1])
			#print self.linkInfo(a[0], a[1])
			
		addEtcHostsMininetConfig(hostsConfigEntries)
				

