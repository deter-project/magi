#!/usr/bin/env python

import logging
import yaml
import optparse
import signal
import time
import sys

import subprocess

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

from magi.orchestrator.parse import *
from magi.messaging import api

logging.basicConfig(level=logging.INFO)
done = False
messaging = None

def handler(signum, frame):
    global done
    print "shutting down ..."
    done = True
    messaging.poisinPill()

if __name__ == '__main__':
	optparser = optparse.OptionParser() 
	optparser.add_option("-c", "--control", dest="control", help="The control node to connect to (i.e. control.exp.proj)")
	optparser.add_option("-g", "--group", dest="group", help="List one of more groups to listen to (ie \"control:data:trigger\") ", default="control")
	optparser.add_option("-n", "--tunnel", dest="tunnel", help="Tell orchestrator to tunnel data through Deter Ops (users.deterlab.net).", default=False, action="store_true")    
	(options, args) = optparser.parse_args()

	signal.signal(signal.SIGINT, handler)
	if options.tunnel:
		localport = 18802
		p = subprocess.Popen("ssh users.deterlab.net -L " + str(localport) + ":" + options.control + ":18808 -N", shell=True)
		messaging = api.ClientConnection("pypassive", "127.0.0.1", localport)
	else:
		messaging = api.ClientConnection("pypassive", options.control, 18808)

	if ':' in options.group:
		for grp in options.group.split(':'):  
			print "Joining group:", grp 
			messaging.join(grp) 
	else:
		print "Joining group:", options.group 
		messaging.join(options.group)

	engine = create_engine('sqlite:////tmp/magi.db')
	metadata = MetaData()
	countertable = Table('pktdata', metadata,
                       	Column('timestamp', Integer),
                       	Column('eth0_pkts', Integer),
                       	Column('eth0_bytes', Integer),
                        Column('eth1_pkts', Integer),
                        Column('eth1_bytes', Integer),
                        Column('eth2_pkts', Integer),
                        Column('eth2_bytes', Integer)
                   )
	metadata.create_all(engine)

	timestamp = 0;
	eth0_pkts = 0;
	eth0_bytes = 0;
	eth1_pkts = 0;
	eth1_bytes = 0;
	eth2_pkts = 0;
	eth2_bytes = 0;

	while not done:
		msg = messaging.nextMessage(True, sys.maxint)  # without at least some timeout main thread stops receiving signals
		print msg

		if msg.contenttype == 0:
			[node, table, pktdata] = msg.data.split(':',2)
			timestamp += 1

			for entry in yaml.load(pktdata):	
				name = entry.get('name');
				if name == 'in-eth0':
					eth0_pkts = entry.get('pkts')
					eth0_bytes = entry.get('bytes')
				elif name == 'in-eth1':
					eth1_pkts = entry.get('pkts')
					eth1_bytes = entry.get('bytes')
				elif name == 'in-eth2':
					eth2_pkts = entry.get('pkts')
					eth2_bytes = entry.get('bytes')

			ins = countertable.insert().values(timestamp=timestamp, eth0_pkts=eth0_pkts, eth0_bytes=eth0_bytes, eth1_pkts=eth1_pkts, eth1_bytes=eth1_bytes, 
							eth2_pkts=eth2_pkts, eth2_bytes=eth2_bytes)
			conn = engine.connect()
			result = conn.execute(ins)
				
		#if msg is not None and type(msg) is not str:
		#	print msg.src, msg.dstgroups, msg.dstdocks, msg.data


	if p:
		p.terminate()
