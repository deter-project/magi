
import socket
import logging
import time
from asyncore import dispatcher
from transport import Transport
import transportStream
from magimessage import DefaultCodec


log = logging.getLogger(__name__)

class TCPServer(Transport):
	""" Simple TCP Server that returns new TCP clients as 'messages' """
	def __init__(self, address = None, port = None):
		Transport.__init__(self)
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind((address, port))
		self.listen(5)

	def handle_accept(self):
		pair = self.accept()
		if pair is None:
			return
		sock, addr = pair
		log.info('Incoming connection from %s', repr(addr))
		newTrans = TCPTransport(sock)
		newTrans.saveHost = addr[0]
		newTrans.savePort = addr[1]
		self.inmessages.append(newTrans)

	def serverOnly(self):
		return True

	def __repr__(self):
		return "TCPServer"
	__str__ = __repr__



class TCPTransport(transportStream.StreamTransport):
	"""
		This class implements a TCP connection that streams MAGI messages back and forth.  It
		uses the StreamTransport for most work, extending it just for the connecting and reconnecting
		portion.
	"""

	def __init__(self, sock = None, codec=DefaultCodec, address = None, port = None):
		"""
			Create a new TCP Transport.  If sock is provided, it is used, otherwise starts with
			an unconnected socket. 
		"""
		transportStream.StreamTransport.__init__(self, sock=sock, codec=codec)
		self.closed = False
		self.saveHost = ""
		self.savePort = -1
		if address is not None and port is not None:
			self.connect(address, port)
			

	def connect(self, host, port):
		"""
			Attempt to connect this socket.
		"""
		self.saveHost = host
		self.savePort = port
		self.closed = False
		log.info("connect %s:%d", self.saveHost, self.savePort)
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		log.info("If connection fails, it will retry shortly.")
		dispatcher.connect(self, (self.saveHost, self.savePort))


	def reconnect(self):
		"""
			Attempt a reconnect of a socket that was closed or never fully connected
		"""
		self.connect(self.saveHost, self.savePort)


	def handle_write(self):
		"""
			Override stream version so we can add hosttime to outgoing packets
		"""
		if self.txMessage.isDone():
			try:
				msg = self.outmessages.pop(0)
				msg.hosttime = int(time.time())
				self.txMessage = transportStream.TXTracker(codec=self.codec, msg=msg)
			except IndexError:
				return

		self.txMessage.sent(self.send(self.txMessage.getData()))


	def __repr__(self):
		return "TCPTransport %s:%d" % (self.saveHost, self.savePort)
	__str__ = __repr__

