
import logging
import errno
import os
import ssl # requires python2.6, TODO, where to put test and to stop import for lower systems?
import sys
from transportTCP import TCPTransport, TCPServer
from magimessage import DefaultCodec

log = logging.getLogger(__name__)

class MyContext(object):
	def __init__(self, matchingCN="node", authOnly=False, **kwargs):
		"""	
			Create a new context for the SSL pieces.
			Required arguments:
				cafile - path to CA certificate 
				nodefile - path to node certificate file and key file
				matchingOU - the subject operating unit to require in the peers verified certificate
			Optional arguments:
				matchCN - the subject common name to match, defaults to "node"
				authOnly - attempt to use eNULL cipher for authentication but no encryption
		"""

		cafile = kwargs.pop('cafile')
		if cafile is None or not os.path.exists(cafile):
			raise OSError(errno.ENOENT, "CA certificate path is invalid: %s" % cafile)

		nodefile = kwargs.pop('nodefile')
		if nodefile is None or not os.path.exists(nodefile):
			raise OSError(errno.ENOENT, "Node certificate path is invalid: %s" % nodefile)

		self.cafile = cafile
		self.nodefile = nodefile
		self.matchOU = kwargs.pop('matchingOU')
		self.matchCN = matchingCN
		self.authOnly = authOnly


	def verifyPeer(self, peercert):
		""" Get the remove cert and make sure we want to talk to them """
		peerOU = ""
		peerCN = ""
		for x in peercert['subject']:
			for y in x:
				if y[0] == 'organizationalUnitName': peerOU = y[1]
				if y[0] == 'commonName': peerCN = y[1]

		if self.matchOU != peerOU:
			raise ssl.SSLError, "OU should be %s but peer is %s" % (self.matchOU, peerOU)
		if self.matchCN != peerCN:
			raise ssl.SSLError, "CN should be %s but peer is %s" % (self.matchCN, peerCN)
		log.info("Verified peer as %s, %s", peerOU, peerCN)


	def wrapSocket(self, sock, serverSide=False):
		""" Return a wrapped SSL socket that acts like a regular socket """
		kwargs = dict()
		kwargs['certfile'] = self.nodefile
		kwargs['ca_certs'] = self.cafile
		kwargs['server_side'] = serverSide
		kwargs['cert_reqs'] = ssl.CERT_REQUIRED
		kwargs['ssl_version'] = ssl.PROTOCOL_TLSv1
		kwargs['do_handshake_on_connect'] = False  # for async ops
		
		ver = sys.version_info
		if (ver[0] == 2 and ver[1] >= 7) or (ver[0] == 3 and ver[1] >= 2):
			if self.authOnly:
				kwargs['ciphers'] = "eNULL"  # Note, totally untested (and eNULL != NULL)

		return ssl.wrap_socket(sock, **kwargs)


class SSLServer(TCPServer):
	"""
		SSL Server that returns new SSL transport as 'messages'
		See MyContext for additional arguments used
	"""
	def __init__(self, address = None, port = None, **kwargs):
		TCPServer.__init__(self, address=address, port=port)
		self.ctx = MyContext(**kwargs)

	def __repr__(self):
		return "SSLServer"
	__str__ = __repr__

	def handle_accept(self):
		pair = self.accept()
		if pair is None:
			return
		sock, addr = pair
		log.info('Incoming connection from %s', repr(addr))
		transport = SSLTransport(sock=sock, ctx=self.ctx)
		transport.saveHost = addr[0]  # so SSLTransport __repr__ works
		transport.savePort = addr[1]
		self.inmessages.append(transport)


class SSLTransport(TCPTransport):
	"""
		This class implements a SSL connection that streams MAGI messages back and forth.  It
		uses the TCPTransport for some pieces, alot goes straight through to StreamTransport.
		See MyContext for additional arguments used
	"""

	def __init__(self, sock = None, codec=DefaultCodec, address = None, port = None, ctx = None, **kwargs):
		"""
			Create a new SSL Transport.
		"""
		TCPTransport.__init__(self)
		if ctx is None:
			self.ctx = MyContext(**kwargs)
		else:
			self.ctx = ctx
		
		self.sslConnected = False  # need to do handshake still
		self.sslWantWrite = False
		if sock is not None:   # passing off from server
			sock.setblocking(0)
			self.set_socket(self.ctx.wrapSocket(sock, serverSide=True))
			self.connected = True
		elif address is not None: 
			self.connect(address, port)


	def handle_connect(self):
		""" Wait to wrap the socket until the connect completes, python ssl doesnt have connect_ex in 2.6 """
		wrapped = self.ctx.wrapSocket(self.socket)
		self.set_socket(wrapped)
		self.handle_ssl()


	def __repr__(self):
		return "SSLTransport %s:%d" % (self.saveHost, self.savePort)
	__str__ = __repr__


	def handle_ssl(self):
		"""
			Take care of special function calls needed during initial handshake
		"""
		try:
			if self.socket._sslobj is None:
				log.warning("calling handle ssl but SSLSocket isn't apparently ready")
				return
			self.socket.do_handshake()
			self.ctx.verifyPeer(self.socket.getpeercert())
			self.sslConnected = True
			self.sslWantWrite = False
		except ssl.SSLError, err:
			if err.args[0] == ssl.SSL_ERROR_WANT_READ: pass
			elif err.args[0] == ssl.SSL_ERROR_WANT_WRITE: self.sslWantWrite = True
			else: raise


	def writable(self):
		""" Also need to check SSL requirements """
		return TCPTransport.writable(self) or self.sslWantWrite

	def handle_write(self):
		if self.sslConnected:
			TCPTransport.handle_write(self)
		else:
			self.handle_ssl()

	def handle_read(self):
		if self.sslConnected:
			TCPTransport.handle_read(self)
		else:
			self.handle_ssl()

	def handle_close(self):
		self.sslConnected = False
		self.sslWantWrite = False
		TCPTransport.handle_close(self)

