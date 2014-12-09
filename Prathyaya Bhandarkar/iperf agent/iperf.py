
from magi.util.agent import DispatchAgent, agentmethod
from magi.util.processAgent import initializeProcessAgent
from subprocess import Popen, PIPE
import logging
import sys
import time
import signal
import os

log =logging.getLogger(__name__)

class Iperf(DispatchAgent):
	def __init__(self):
		DispatchAgent.__init__(self)
		self.clientFile = '/tmp/iperfClientOutput.txt'
		self.serverFile = '/tmp/iperfServerOutput.txt'
		self.pid = None
		
	@agentmethod()
	def startClient(self,msg,server,port=None,time=10,bw=None):
		print(server, port, time)
		log.info("starting iperf client")
		if not server:
			raise AttributeError("iperf server not set")
		log.info("Server: %s" %(server))
		if port != None and bw !=None:
			iperfCmd = ['iperf', '-c', server, '-u', '-t', time, '-p', port, '-b', bw]
		elif port != None and bw == None:
			iperfCmd = ['iperf', '-c', server, '-u', '-t', time, '-p', port]
		elif port == None and bw!= None:
			iperfCmd = ['iperf', '-c', server, '-u', '-t', time, '-b', bw]
		else:
			iperfCmd = ['iperf', '-c', server, '-t', time]
		log.info(iperfCmd)
		p = Popen(iperfCmd, stdout=PIPE, stderr=PIPE)
		if p.wait():
			log.error('Could not start iperf')
			[out,err] = p.communicate()
			log.error(err)
			return False
		log.info('success')
		[out,err] = p.communicate()
		f = open(self.clientFile, 'w')
		f.write(out)
		f.close()
		log.info('client job done')
		return True
		
	@agentmethod()	
	def startServer(self,msg,port=None):
		log.info("starting iperf server")
		#if not server:
			#raise AttributeError("iperf server not set")
		if port != None:
			iperfServerCmd = ['iperf', '-s', '-u', '-p', port]
		else:
			iperfServerCmd = ['iperf', '-s', '-u']
		log.info(iperfServerCmd)
		p = Popen(iperfServerCmd, stdout=PIPE, stderr=PIPE)
		time.sleep(1)
		if p.poll() != None:
			log.info('Could not start iperf')
			raise OSError('could not start iperf server')
			#return False
		self.pid = p.pid
		log.info(self.pid)
		#[out,err] = p.communicate()
		#log.info(out)
		#f = open(self.serverFile, 'w')
		#f.write(out)
		#f.close()
		log.info('iperf server started with process id %d' %(self.pid))
		return True

	@agentmethod()	
	def stopServer(self,msg):
		log.info("stopping iperf server")
		log.info("killing iperf server process %d", self.pid)
		if self.pid:
			os.kill(self.pid, signal.SIGTERM)
		return True

def getAgent(**kwargs):
	log.info(kwargs)
	agent = Iperf()
	agent.setConfiguration(None, **kwargs)
	return agent
	
if __name__ == "__main__":
	agent = Iperf()
	initializeProcessAgent(agent, sys.argv)
	agent.setConfiguration(None, **kwargs)
	agent.run()