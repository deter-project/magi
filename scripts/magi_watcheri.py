#!/usr/bin/python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

# watcher
import time
import sys
import re
import subprocess
from optparse import OptionParser
# import socket

# magi
import signal
from magi.messaging import api

signal.signal(signal.SIGINT, signal.SIG_DFL)

def doMainLoop():

	# create the message handler
 	handler=MessageHandler()

	handler.loadRoutes(options.routeFile)

	# Connect to network as node "watcheri"
	conn = api.ClientConnection("watcheri", options.host, 18808)
	
	# Request to receive messages to group "data"
	conn.join("data")

	# Run forever
	done = False
	while not done:
		# block until next message
		msg = conn.nextMessage(True)
	
		# if the destination dock indicates a worm message
		if 'worm' in msg.dstdocks:
			# do something with the lines of text here
			textlines = msg.data
			if options.verbose: 
				print "incoming data:"
				print textlines

			lines=textlines.split('\n')
			for l in lines:
				data=parseLine(l)

		 		if len(data)!=4:
		 			print "Parse error on line: %s" % l
		 			continue
		 
		 		handler.handleMessage(data[2], data[0], data[1], data[3])

class MessageHandler:
	"""Parse incoming messages and generate Watcher messages as output."""
	def __init__(self):
		# Map of names to addresses - watcher only speaks in IP addrs.
		# Since we don't have DNS here, just make up addresses per name.
		self.addrmap={}
		# map between event type and handler function. 
		# These are all the event types in the dat files I have, 
		# but I'm not sure how to represent them all. 
		self.funcs={
			"ASNUM" : self.handleAsNum,
			"BOT_STATE" : self.handleBotState,  
			"DEFENSE" : self.handleDefense,
			"DONE" : self.handleDone,
			"DOS" : self.handleDOS,
			"INFCOUNT" : self.handleInfCount,
			"INGRESS" : self.handleIngress,
			"LOCATION" : self.handleLocation,
			"PKTRATE" : self.handlePktRate,
			"ROUTES" : self.handleRoutes,
			"TYPES" : self.handleTypes,
		}
		# make sure the send* watcher command are available.
		# GTL - this does not work as the send* binaries incorrectly
		# return 1 when --help is the argument. Oh well.
		# self.executeCmd("sendEdgeMessage --help") 

		# Keep track of routes so we can draw paths
		# routes is a dict of dict {host, {nexthop, destination}}
		self.routes={}

	def loadRoutes(self, routeFile):
		cnt=0
		cur_src=None
		with open(routeFile, "r") as f:
			for line in f:
				if line[0] != " ":   # new src
					cur_src=line.strip()	
					if cur_src not in self.routes:
						self.routes[cur_src]={}
				else:
					l=line.strip()
					(dest,nexthop)=l.split(' ')
					if not cur_src:
						print "Warning: error parsing routes file"
						next
					self.routes[cur_src][dest]=nexthop
					cnt=cnt+1
					if options.verbose:
						print "(from file) route: added %s -> %s via %s to route table" % (cur_src, dest, nexthop)
		
		print "loaded %s routes from %s" % (cnt, routeFile)

	def getAddress(self, name):
        # # Is it worthwhile to cache addresses? 
		# try:
		# 	addr=socket.gethostbyname("%s.%s" % (name, options.domain))
		# except socket.gaierror as e:
		# 	print "Error getting hostname for %s." % name
		# 	addr=None
		# return addr

		if not name in self.addrmap:
			id=len(self.addrmap)
			self.addrmap[name]="192.168." + str(1+(id/255)) + "." + str(1+(id%255))
			if options.verbose:
				print "New address created for %s : %s" % (name, self.addrmap[name])
		return self.addrmap[name]

	def handleMessage(self, eventType, timestamp, name, params):
		if options.verbose:
			print "Handling message type: %s" % eventType

		addr=self.getAddress(name)

		# set the node label - may be wasteful to send a new message each time?
		# May want to toggle this via command line arg?
		# cmd="sendNodePropertiesMessage -s %s --label %s --node %s" % (options.server, name, addr); 
		# self.executeCmd(cmd); 
		
		# Fire off the appropriate message by event type.
		if eventType not in self.funcs:
			print "Warning: got unknown message type: %s" % eventType
		else:
			self.funcs.get(eventType)(timestamp, addr, name, params)

	def handleAsNum(self, timestamp, addr, name, params):
		# is always of the form: 
		# 1326926751 botmaster ASNUM [name] active
		pass

	def handleBotState(self, timestamp, addr, name, params):
		# only seems to be RESET and IRC. ?
		pass

	def handleDone(self, timestamp, addr, name, params):
		# Dont' know about this one. Maybe something to do with the
		# botmaster attack? name is always "botmaster" or "me"
		pass

	def handleDOS(self, timestamp, addr, name, params):
		# start of DOS I'm guessing. Format is like:
		# 1326917989 bot6 DOS 1 1 START defender1
		# may be: bot6 starts DOS on defender1 at timestamp? Not sure what the 
		# int params are, attack settings? 
		# May be try just drawing the edge and having it timeout? 
		if len(params) is not 4:
			print "Warning: got unknown format for DOS args, should be 4 args. Instead got %s" % " ".join(params)
			return
		dest=params[3]
		self.showPath(name, dest)

	def showPath(self, head, tail):
		if options.verbose:
			print 'Showing path between %s (%s) and %s (%s)' % (head, self.getAddress(head), tail, self.getAddress(tail))
		# Use a unique ID to label the path, so it's easier to see.
		pathid=self.getAddress(head).split('.')[3]
		# loop through host, nexthop pairs until we hit the destination. Draw edge for each link.
		while head != tail:
			if head not in self.routes or tail not in self.routes[head]: 
				print 'Warning: I do not have a full route between %s and %s.' % (head, tail)
				break
			nexthop=self.routes[head][tail]
			# (GTL - looks like color is broken on the GUI end. Can be set manually for the 
			# layer though.)
			cmd="sendEdgeMessage -s %s -h %s -t %s -y DOS -x 5000 -c red -l %s" % (options.server, self.getAddress(head), self.getAddress(nexthop), pathid)
			self.executeCmd(cmd)
			head=nexthop

	def handlePktRate(self, timestamp, addr, name, params):
		if len(params) is not 1:
			print "Warning: got unknown format for PKTRATE args, should be single arg, the packet rate."
			return
		cmd="sendDataPointMessage -s %s -n %s -g PacketRate -d %s" % (options.server, addr, str(int(params[0])))
		self.executeCmd(cmd)

	def handleInfCount(self, timestamp, addr, name, params):
		# ex: 1326917991 botmaster INFCOUNT 6
		if len(params) is not 1:
			print "Warning: got unknown format for INFCOUNT args, should be single arg, the infection count."
			return
		cmd="sendDataPointMessage -s %s -n %s -g InfectionCount -d %s" % (options.server, addr, str(float(params[0])))
		self.executeCmd(cmd)

	def handleTypes(self, timestamp, addr, name, params):
		if len(params) is not 1:
			print "Warning: got unknown format for TYPES args, should be single arg, the type."
			return
		nodeType=params[0]
		# GTL - spaces in labels seem to cause trouble. All after the space is dropped. 
		# label="%s_(%s)" % (name, nodeType)
		(label,color,shape)=('','','')
		t=params[0]
		if t == 'bot':
			(label,color,shape)=('bot','yellow','circle')
		elif t == 'botmaster':
			(label,color,shape)=('master','red','torus')
		elif t == 'dest':
			(label,color,shape)=('dest','green','circle')

		if label:
			host_addr=self.getAddress(name)
			if not host_addr:
				print "Error getting address for name: %s" % name
			return 

			l="%s_(%s)" % (host_addr.split('.')[3], label)
			cmd="sendNodePropertiesMessage -s %s --label %s --node %s" % (options.server, l, addr); 
			self.executeCmd(cmd); 
		if color:
			cmd="sendNodePropertiesMessage -s %s --color %s --node %s" % (options.server, color, addr); 
			self.executeCmd(cmd); 
		if shape:
			cmd="sendNodePropertiesMessage -s %s --shape %s --node %s" % (options.server, shape, addr); 
			self.executeCmd(cmd); 

	def handleLocation(self, timestamp, addr, name, params):
		# send a watcherGPS message
		if len(params) is not 2 and len(params) is not 3:
			print "Warning: got unknown format for LOCATION args, should be X Y [name]. Got: %s" % " ".join(params)
			return
		z=0  # no altitude
		if len(params) is 3:
			addr=self.getAddress(params[2])  # if we have an extra param, it's the name of the node whose location this is.
		cmd="sendGPSMessage -s %s -x %s -y %s -z %s -n %s" % (options.server, params[0], params[1], z, addr) 
		self.executeCmd(cmd)
	
	def handleIngress(self, timestamp, addr, name, params):
		# I don't know what this message means
		# For now, put it in an edge on a layer.
		# ex: 1326917703 defender2 INGRESS di2
		if len(params) is not 1:
			print "Warning: got unknown format for INGRESS args, should be 1 args. Instead got %s" % " ".join(params)
			return
		host=params[0]
		host_addr=self.getAddress(host)
		cmd="sendEdgeMessage -s %s -h %s -t %s -y Ingress" % (options.server, addr, host_addr)
		self.executeCmd(cmd)
	
	def handleRoutes(self, timestamp, addr, name, params):
		# should be one param of format "dest:nexthop,dest:nexthop,..."
		if len(params) is not 1:
			print "Warning: got badly formatted ROUTES parameters: %s" % " ".join(params)
			return
		if name not in self.routes:
			self.routes[name]={}
		# parse all given path
		nbrs=[]   
		for path in params[0].split(','):
			(dest,nexthop)=path.split(':')
			nbraddr=self.getAddress(nexthop)
			if nbraddr == addr:  # ignore path to self
				next
			nbrs.append(nbraddr)
			# add route from name to dest via nexthop to route table
			if dest not in self.routes:
				self.routes[name][dest]=None
			self.routes[name][dest]=nexthop
			if options.verbose:
				print "route: added %s -> %s via %s to route table" % (name, dest, nexthop)
		tmp=sorted(set(nbrs))  # remove possible dups, though Watcher doesn't care. 
		cmd="sendConnectivityMessage -s %s -f %s -l neighbors %s" % (options.server, addr, " ".join(tmp))
		self.executeCmd(cmd)

	def handleDefense(self, timestamp, addr, name, params):
		# single param which is a comma separated list of machine names. 
		# don't know what it means though. 
		if len(params) is not 1:
			print 'Warning: unknown format for DEFENSE message: %s' % " ".join(params)
			return
		addrs=[self.getAddress(name) for name in params[0].split(',')]   # list of addresses from names via comma separated string.
		cmd="sendConnectivityMessage -s %s -f %s -l defense %s" % (options.server, addr, " ".join(addrs))
		self.executeCmd(cmd)

	def executeCmd(self, cmd):
		if options.verbose:
			print "sending cmd: %s" % cmd
		if options.testrun:
			print "execute cmd: %s" % cmd
			return
		# GTL - check return
		try:
			# platform independent way to redirect to /dev/null? 
            # subprocess.check_call(cmd.split(), stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT) 
			subprocess.call(cmd.split(), stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT) 
		except OSError as e:
			print "OS Error executing command: %s" % e
			print "Bad command: %s" % cmd
			sys.exit(1)
		except ValueError as e:
			print "Value error executing command: %s" % e
			print "Bad command: %s" % cmd
			sys.exit(1)
		except subprocess.CalledProcessError as e:
			print "Called process error executing command: %s" % e
			print "Bad command: %s" % cmd
			sys.exit(1)

def generateLines(f):
	for l in f:
		yield l.strip() 

def parseLine(l):
	"""Return structured data given a line of input.
	data:
	(time, node_name, event_type, [param1, param2, param3])
    """
	m=re.search("([^ ]+) ([^ ]+) ([^ ]+) (.*)$", l)
	if m:
		if options.verbose:
			print("Parseline: time: %s, name: %s, event: %s, args: %s" % 
				(m.group(1), m.group(2), m.group(3), m.group(4)))
		return (int(m.group(1)), m.group(2), m.group(3), m.group(4).split())
	else:
		if options.verbose:
			print("Parseline: ignoring misparsed line \"%s\"" % l)
		return ()

if __name__ == '__main__':
	desc = """Parse event logs and generate Watcher messages."""
	# argparser = argparse.ArgumentParser(description=desc)
	argparser = OptionParser() 
	argparser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Turn on verbose output")
	argparser.add_option("-s", "--server", dest="server", help="watcherd server name or address. Required argument.")
	argparser.add_option("-t", "--testrun", dest="testrun", action="store_true", help="If given, don't execute commands, but print out what would've happened.")
	argparser.add_option("-m", "--montageHost", dest="host", help="The montage backend host to connect to.") 
	argparser.add_option("-d", "--domain", dest="domain", help="The experiment.groupid appended to test node names")
	argparser.add_option("-r", "--routes", dest="routeFile", help="Cheat sheet for routes in one convient file.")
	(options, args) = argparser.parse_args()

	if not options.host:
		options.host="127.0.0.1"
	if not options.server:
		options.server="localhost"
	if not options.domain:
		options.domain="bd.detertest"
	
	doMainLoop()


