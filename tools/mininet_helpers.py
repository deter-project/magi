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

for i in range (200):
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
		
		inetSwitch = self.addSwitch('z1')
		
		nodes = topoGraph.nodes()
		nodes.sort()
		bridgeNode = nodes[0]
		for node in nodes:
			if 'control' == node.lower():
				bridgeNode = 'control'
				break

		#Creates mininet hosts that can route packets and generates list of interfaces necessary for each host
		for nodeName in topoGraph.nodes():
			
			node = self.addNode(name=nodeName, cls=LinuxRouter, ip='')
			#self.addLink(inetSwitch, node)
			
			neighbors = topoGraph.neighbors(nodeName)
			nodeIntfList = deque([])
			for i in range(len(neighbors)):
				intfname = '%s-eth%s'%(nodeName, i+1)				
				nodeIntfList.append(intfname)
			interfacelist[nodeName] = nodeIntfList
			
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
			
			nodeConfigDir = localInfo['configDir']
			helpers.makeDir(nodeConfigDir)
			nodeConfFile = "%s/node.conf" %(nodeConfigDir)
			helpers.writeYaml(nodeConfig, nodeConfFile)
			log.info("Created a node configuration file at %s", nodeConfFile)
			
		print "Interface List: %s" % interfacelist.items() 
			
		#adds edges in mininet topology and configures Ip addresses
		hostsConfigEntries = []
		for e in topoGraph.edges():
			#node1, node2 = self.get(e[0]), self.get(e[1]) 
			edgeNodes = []
			edgeNodes.append(e[0])
			edgeNodes.append(e[1])
			edgeNodes.sort()
			
			print "adding link between %s and %s" %(edgeNodes[0], edgeNodes[1])
			
			node1InterfaceList = interfacelist[edgeNodes[0]]
			node2InterfaceList = interfacelist[edgeNodes[1]]	
			
			node1Interface = node1InterfaceList.popleft()
			node2Interface = node2InterfaceList.popleft()
					
			print "node1Interface name = %s, node2Interface name = %s" %(node1Interface, node2Interface)
			
			ip1 = iplist.popleft()
			ip2 = iplist.popleft()
			
			print "ip1 = %s, ip2 = %s" %(ip1, ip2)
			
			#link1 = Link.__init__(e[0], e[1], intfName1=node1Interface, intfName2=node2Interface, addr1=iplist.popleft(), addr2=iplist.popleft())
			self.addLink( edgeNodes[0], edgeNodes[1], intfName1=node1Interface, intfName2=node2Interface, params1={'ip': ip1 }, params2={ 'ip' : ip2 } )
			
			hostsConfigEntries.append("%s	%s-%s %s" %(ip1.split('/')[0], edgeNodes[0], edgeNodes[1], edgeNodes[0]))
			hostsConfigEntries.append("%s	%s-%s %s" %(ip2.split('/')[0], edgeNodes[1], edgeNodes[0], edgeNodes[1]))
			print "\n\nadded link between %s and %s\n\n" %(edgeNodes[0], edgeNodes[1])
			#print self.linkInfo(a[0], a[1])
			
		itr = 1
		for nodeName in topoGraph.nodes():
			nodeIntf = '%s-eth0'%(nodeName)
			switchIntf = 's1-eth%s'%(nodeName)
			nodeIP = '10.1.%d.1/24'%(itr)
			switchIP = '10.1.%d.2/24'%(itr)
			
			print "nodeIntf name = %s, switchIntf name = %s" %(nodeIntf, switchIntf)
			print "nodeIP name = %s, switchIP name = %s" %(nodeIP, switchIP)
			
			self.addLink( nodeName, 'z1', intfName1=nodeIntf, params1={'ip': nodeIP }, params2={ 'ip' : switchIP } )
			itr += 1
			
		
		addEtcHostsMininetConfig(hostsConfigEntries)
				

