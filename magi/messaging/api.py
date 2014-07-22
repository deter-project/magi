
from magi.util.Collection import namedtuple
from magi.messaging.transportTCP import TCPServer, TCPTransport
from magi.messaging.transportSSL import SSLServer, SSLTransport
from magi.messaging.transportMulticast import MulticastTransport
from magi.messaging.magimessage import MAGIMessage, DefaultCodec
from magi.messaging.transportPipe import InputPipe, OutputPipe
from magi.messaging.transportTextPipe import TextPipe

import Queue
import yaml
import os

GroupRequest = namedtuple("GroupRequest", "type, group, caller")
TransportRequest = namedtuple("TransportRequest", "transport, keepConnected")
TransmitRequest = namedtuple("TransmitRequest", "msg, args")
MessageStatus = namedtuple("MessagingStatus", "status, isack, msg")

class Messenger(object):
	"""
		The basic messenger interface for clients.  It provides the run loop for async transports and their scheduling
		as well as routing behaviour if there are multiple transports.
		THe user can join/leave groups as well as receive and send messages.
		This is the interface used by the caller.  Its passes most items as objects in the TxQueue as most operations take
		place in another thread.
	"""

	def __init__(self, name):
		""" Creating messaging interface here """
		self.name = name
		self.rxqueue = Queue.Queue()
		self.txqueue = Queue.Queue()
		self.thread = None
		self.startDaemon()

	def startDaemon(self):
		"""
			Start the thread that takes care of polling and scheduling for all the transports
		"""
		from worker import WorkerThread
		self.thread = WorkerThread(self.name, self.txqueue, self.rxqueue)
		self.thread.start()
		
	def addTransport(self, transport, keepConnected=False):
		"""
			Add a new transport to this messages.  This can be compared to adding a new ethernet interface on a simple node.
			The messenger will take care of routing if there are multiple transports.  Examples:

			TCPServer('0.0.0.0', 17708)
			TCPTransport('192.168.1.50', 17708)
			MulticastTransport('192.168.1.40', '255.239.13.4')
		"""
#		self.txqueue.put(TransportRequest(transport, keepConnected))
		self.thread.addTransport(transport, keepConnected)

	def join(self, group, caller = "default"):
		""" 
			Join the group 'group' (a string value).
			Messages sent to this group by other nodes will now be received by the owner of this messaging object
			The call option allows one to attach an 'ID' to the join request so that someone else joining and leaving
			the same group doesn't cause the official leave to actually occur.
		"""
#		self.txqueue.put(GroupRequest("join", group, caller))
		self.thread.processGroupRequest(GroupRequest("join", group, caller))

	def leave(self, group, caller = "default"):
		"""
			Leave the group 'group' (a string value).
			This messaging object will no longer receive messages for this group.
			If the caller joined with a caller value, it must use the same value for the leave request
		"""
#		self.txqueue.put(GroupRequest("leave", group, caller))
		self.thread.processGroupRequest(GroupRequest("leave", group, caller))
	
	def nextMessage(self, block=False, timeout=None):
		"""
			Called to remove a received message from the queue.  block and timeout are as specified in
			Queue.Queue.get()
		"""
		return self.rxqueue.get(block, timeout)

	def send(self, msg, **kwargs):
		"""
			Enqueue a message for transmittal, kwargs is a list of delivery request values to specify desired delivery behavior
		"""
		self.txqueue.put(TransmitRequest(msg, kwargs))

	def trigger(self, **kwargs):
		"""
			Send a trigger event.  Single location for a common action, ick though, this is application level stuff in messaging code
		"""
		self.send(MAGIMessage(groups="control", docks="daemon", data=yaml.dump(kwargs), contenttype=MAGIMessage.YAML))
		
	def poisinPill(self):
		"""
			Puts a "PoisinPill" string into the rxqueue so that the first listener on nextMessage will
			wake up and get the message.
		"""
		self.rxqueue.put("PoisinPill")

	def stop(self):
		self.thread.stop() # stopping the worker thread
	
		
class ClientConnection(Messenger):
	""" Wrapper to provide the basic TCP client interface """
	def __init__(self, name, host, port):
		Messenger.__init__(self, name)
		conn = TCPTransport(address=host, port=port)
		self.addTransport(conn, True)
#		self.startDaemon()

class SSLClientConnection(Messenger):
	""" Wrapper to provide the basic SSL client interface """
	def __init__(self, name, host, port, project, experiment):
		Messenger.__init__(self, name)
		keydir = "/proj/%s/exp/%s/tbdata/" % (project, experiment)
		conn = SSLTransport(address=host, port=port, cafile=os.path.join(keydir,'ca.pem'), nodefile=os.path.join(keydir,'node.pem'), matchingOU="%s.%s"%(project,experiment))
		self.addTransport(conn, True)
#		self.startDaemon()

