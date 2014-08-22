
import sys
import struct
import logging
from transport import Transport
from magimessage import DefaultCodec

log = logging.getLogger(__name__)
debug = False

class TXTracker(object):

	def __init__(self, msg = None, codec = DefaultCodec):
		if msg is None:
			self.dataDone = True
			return

		self.header = StreamTransport.PREAMBLE + codec.encode(msg)
		self.headerDone = False
		self.hindex = 0

		if msg.data is None:
			self.data = ""
		else:
			self.data = msg.data
		self.dataDone = False
		self.dindex = 0

	def isDone(self):
		return self.dataDone
		
	def getData(self):
		if self.dataDone:
			raise IndexError("No more data")
		elif self.headerDone:
			return self.data[self.dindex:]
		else:
			return self.header[self.hindex:]

	def sent(self, count):
		if debug: log.debug("%d bytes sent", count)
		if self.dataDone:
			raise IndexError("Already completed, this doesn't make sense")

		elif self.headerDone:
			self.dindex += count
			if self.dindex >= len(self.data):
				if debug: log.debug("Data send complete")
				self.dataDone = True
		else:
			self.hindex += count
			if self.hindex >= len(self.header):
				self.headerDone = True
				if debug: log.debug("Header send complete")
				# self.dataDone = True ##  For testing partial transfers



class RXTracker(object):

	def __init__(self, startbuf = "", codec = DefaultCodec):
		self.preamblebuf = ""
		self.preambledone = False

		self.hdrlen = 0
		self.hdrbuf = ""
		self.headerdone = False

		self.datalen = sys.maxint
		self.datalist = list()
		self.leftover = list()

		self.codec = codec 
		self.processData(startbuf)
		

	def processData(self, data):
		if data is None or len(data) == 0:
			return

		if debug: log.debug("processing %d bytes (%s, %s)", len(data), self.preambledone, self.headerdone)

		if not self.preambledone:  # Reading into preamble
			self.preamblebuf += data
			data = None
			pidx = self.preamblebuf.find(StreamTransport.PREAMBLE)

			if pidx < 0:
				# delete what we can easily from head of buffer for faster search next time
				if len(self.preamblebuf) > StreamTransport.PREAMBLELEN:
					self.preamblebuf = self.preamblebuf[-StreamTransport.PREAMBLELEN:]
				return

			self.hdrbuf = self.preamblebuf[pidx + StreamTransport.PREAMBLELEN:]
			if debug: log.debug("Preamble complete")
			self.preambledone = True


		if self.preambledone and not self.headerdone: # Reading into header
			if data:  # still have data to read
				self.hdrbuf += data
				data = None
			try:
				# extraction of lengths may happen multiple times, but keeps code simpler
				(totallen, self.hdrlen) = struct.unpack('>IH', self.hdrbuf[:6])
				self.datalen = totallen - self.hdrlen - 2  # totalen [headerlen] [header] [data], totallen ecompases all []

				left = self.hdrlen - len(self.hdrbuf) + 6
				if left <= 0:
					if debug: log.debug("Header complete")
					self.headerdone = True # Got it all

				if left >= 0:
					if debug: log.debug("Need %d more bytes for header", left)
					return  # More data needed to do anymore so return now

				# otherwise, got first portion of data as well
				self.datalist.append(self.hdrbuf[6+self.hdrlen:])
			except struct.error:
				return


		if data:  # Reading into data
			if debug: log.debug("Appended %d byte to datalist", len(data))
			self.datalist.append(data)


	def isDone(self):
		mylen = sum([len(x) for x in self.datalist])
		if debug: log.debug("Checking if done (%s, %s, have %d vs %d needed)", self.preambledone, self.headerdone, mylen, self.datalen)
		return self.preambledone and self.headerdone and mylen >= self.datalen

	def getMessage(self):
		try:
			mylen = sum([len(x) for x in self.datalist])
			#self.leftover should be used
			#leftover = list()
			while mylen > self.datalen:  # Pull out left over data
				self.leftover.insert(0, self.datalist.pop(-1))
				mylen = sum([len(x) for x in self.datalist])

			if mylen < self.datalen:  # Need part of that first (last) buffer
				hh = self.leftover.pop(0)
				mid = self.datalen - mylen
				self.datalist.append(hh[:mid])
				self.leftover.insert(0, hh[mid:])
				
				
			newmsg, hdrsize = self.codec.decode(self.hdrbuf)
			newmsg.data = ''.join(self.datalist)
			return newmsg
		except:
			log.error("Error decoding message", exc_info=1)
			return None

	def getLeftover(self):
		if debug: log.debug("%d bytes leftover", sum([len(x) for x in self.leftover]))
		return ''.join(self.leftover)




class StreamTransport(Transport):
	"""
		This class implements a basic stream based transport for sending and receiving messages on a serial
		channel.  It uses the default provided codec for serialization but another version can be given 
	"""

	PREAMBLE = "MAGI\x88MSG"
	PREAMBLELEN = len(PREAMBLE)

	def __init__(self, sock = None, codec=DefaultCodec):
		"""
			Create a new TCP Transport.  If sock is provided, it is used, otherwise starts with
			an unconnected socket. 
		"""
		Transport.__init__(self, sock=sock, codec=codec)
		self.closed = False
		if not sock:
			self.connected = False
		self.txMessage = TXTracker(codec=self.codec)
		self.rxMessage = RXTracker(codec=self.codec)


	def setCodec(self, codec):
		Transport.setCodec(self, codec)
		self.rxMessage.codec = codec

		
	def handle_read(self):
		"""
			select indicates that we have data, this will do the actual reading and processing
		"""
		self.rxMessage.processData(self.recv(4096))
		while self.rxMessage.isDone():  # Extract all messages that are in the buffer
			if debug: log.debug("StreamTransport: New message received on %s", self)
			self.inmessages.append(self.rxMessage.getMessage())
			self.rxMessage = RXTracker(startbuf=self.rxMessage.getLeftover(), codec=self.codec)


	def handle_write(self):
		"""
			select indicates that we can write, this will attempt to write whatever we have around
		"""
		if self.txMessage.isDone():
			try:
				self.txMessage = TXTracker(codec=self.codec, msg=self.outmessages.pop(0))
			except IndexError:
				return

		#keep sending till you can
		while not self.txMessage.isDone():
			bytesWritten = self.send(self.txMessage.getData())
			self.txMessage.sent(bytesWritten)
			#if no more can be written, break out
			if bytesWritten == 0:
				break


	def readable(self):
		"""
			Determine if select should check for readability.  We only say yes if we are connected.
		"""
		return self.connected


	def writable(self):
		"""
			Determine if select should check for writability.  We say yes if there are any messages
			in the queue to transmit.  We also say yes if we aren't connected.  During an async connect
			this is the only way for the connection result to get back to us
		"""
		return not self.connected or not self.txMessage.isDone() or len(self.outmessages) > 0


	def __repr__(self):
		return "StreamTransport %s:%d" % (self.saveHost, self.savePort)
	__str__ = __repr__

	def handle_error(self):
		log.error("Error on %s", self)
		self.handle_close()

	def handle_close(self):
		log.info("closing transport")
		self.close()
		self.closed = True  # let upper levels know what happened

