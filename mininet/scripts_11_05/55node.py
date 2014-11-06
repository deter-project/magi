#!/usr/bin/python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.cli import CLI
import os
import time

class SingleSwitchTopo(Topo):
    "Single switch connected to n hosts."
    def __init__(self, c=2, s=2, **opts):
        Topo.__init__(self, **opts)
        switch1 = self.addSwitch('r1')
        for a in range(c):
            host = self.addHost('client%s' % (a + 1))
            self.addLink(host, switch1)
            directory = "/tmp/client%s" % (a + 1)
            if not os.path.exists(directory):
                os.makedirs(directory)
                os.makedirs("%s/tmp" % directory)
            client = 'client%s' % (a + 1)
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
		conf.write("  sensorToCollectorMap: {"+client+": client1}\n")
		conf.write("  configHost: server1\n")
		conf.write("transports:\n- {address: 10.0.0.51, class: TCPTransport, port: 28808}\nprocessAgentsCommPort: 18810")
            conf.close()
                    	
	switch2 = self.addSwitch('r2')
	tcpserver = 1
	for p in range(s):
	    host = self.addHost('server%s' % (p +1))
	    self.addLink(host, switch2)
            directory = "/tmp/server%s" % (p + 1)
            if not os.path.exists(directory):
                os.makedirs(directory)
                os.makedirs("%s/tmp" % directory)
            server = 'server%s' % (p +1)
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
		conf.write("  sensorToCollectorMap: {"+server+": server1}\n")
		if not tcpserver:
			conf.write("  configHost: client1\n")
		conf.write("software:\n")
		conf.write("- {dir: /home/seth, type: source}\n")
		conf.write("- {type: apt}\n")
		if not tcpserver:
			conf.write("transports:\n- {address: 10.0.0.51, class: TCPTransport, port: 28808}\nprocessAgentsCommPort: 18810")
		tcpserver = 0
		conf.close()
	self.addLink(switch1, switch2)	 

def perfTest():
    "Create network and run simple performance test"
    topo = SingleSwitchTopo(c=50, s=5)
    net = Mininet(topo=topo,
                  host=CPULimitedHost, link=TCLink)
    net.start()
    print "Dumping host connections"
    dumpNodeConnections(net.hosts)
    print "Testing network connectivity"
    net.iperf()
    for p in range(5):
    	h1 = net.getNodeByName('server%s' % (p+1))
    	print "running daemon on %s" % h1
    	h1.cmd('/home/seth/magi/scripts/magi_daemon.py --nodeconf /tmp/server%s/server%s.conf -l DEBUG &' % (p+1, p+1) )
        time.sleep(10)
    for p in range(50):
    	h1 = net.getNodeByName('client%s' % (p+1))
    	print "running daemon on %s" % h1
    	h1.cmd('/home/seth/magi/scripts/magi_daemon.py --nodeconf /tmp/client%s/client%s.conf -l DEBUG &' % (p+1, p+1) )
    #net.pingAll()
    CLI(net)
    #c1, s1 = net.getNodeByName('c1', 's1')
    #mnet.iperf((c1, s1))
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    perfTest()
