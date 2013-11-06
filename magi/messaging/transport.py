
import socket
from asyncore import dispatcher
from magi.messaging.magimessage import DefaultCodec

class Transport(dispatcher):
	"""
		Base class for all transports (TCP, multicast, etc).  It inherits from asyncore.dispatcher but overrides some
		of that behaviour to eliminate any automatic map additions.
	"""

	def __init__(self, sock=None, codec=DefaultCodec):
		""" Base transport initializations """
		dispatcher.__init__(self, sock)
		self.codec = codec
		self.inmessages = list()   # list for messaging coming in from media
		self.outmessages = list()  # list of messages to go out to media
		self.sched = None

	def serverOnly(self):
		""" Returns true if this transport doesn't actually tx/rx any messages """
		return False

	def setCodec(self, codec):
		""" Set the encoder/decoder used for serializing the base MAGI message """
		self.codec = codec

	def setScheduler(self, scheduler):
		""" Allow outside entity to tell given us the scheduler to use """
		self.sched = scheduler

	# Change asyncore behaviour to detach map actions, take away _fileno and _map

	def fileno(self):
		""" Get the file number of the socket we are wrapping.  Allowed override by non socket based file descriptors (pipes) """
		return self.socket.fileno()

	def create_socket(self, family, type):
		""" Create a socket and set the attr direct, no map stuff """
		self.family_and_type = family, type
		self.socket = socket.socket(family, type)
		self.socket.setblocking(0)

	def set_socket(self, sock, map=None):
		""" Just set the socket and leave it, no map adding """
		self.socket = sock

	def close(self):
		self.socket.close()

	def handle_connect(self):
		pass  # git rid of error warnings

