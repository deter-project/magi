
import logging
import os
import sys
import errno
import transportStream
from magimessage import MAGIMessage
from transport import Transport

log = logging.getLogger(__name__)


class DevNullList():
	""" provide the outmessages interface but just drop them """
	def append(self, ignore):
		pass

def makeDir(name):
	try:
		os.mkdir(name)
	except OSError, e:
		if e.errno == errno.EEXIST: return
		log.warning("Couldn't create FIFO dir: %s", e)

def makePipe(name):
	try:
		os.mkfifo(name)
	except OSError, e:
		if e.errno == errno.EEXIST: return
		log.warning("Couldn't create FIFO file: %s, %s", name, e)


class TextPipe(Transport):
	""" Dumb pipe that assumes text messages are coming in as ascii text and nothing else """

	def __init__(self, filename, src, srcdock, dstgroups, dstdocks):
		"""
			Create a new input pipe transport.  Need to provide the parameters that are used
			to fill in the message.

			filename - the file name of the pipe
			src - the src for the messages
			srcdock - the srcdock for the messages
			dstgroups - the destination groups for the messages
			dstdocks - the destination docks for the messages
		"""
		Transport.__init__(self)
		self.filename = filename
		self.src = src
		self.srcdock = srcdock
		self.dstgroups = dstgroups
		self.dstdocks = dstdocks

		makeDir('/var/run/magipipes') # make sure it exists
		makePipe(filename) # ditto

		self.fd = os.open(self.filename, os.O_NONBLOCK | os.O_RDONLY)
		self.connected = True
		self.buf = ""
		self.outmessages = DevNullList()

	def close(self):
		os.close(self.fd)
		self.closed = True  # let upper levels know what happened
		
	def fileno(self):
		return self.fd

	def handle_close(self):
		log.info("closing %s", self)
		self.close()

	def handle_read(self):
		""" Override handle_read as this is an FD, not a socket and different data """
		data = os.read(self.fd, 4096)
		if len(data) == 0:  # EOF, someone closed the other end, reopen and wait
			os.close(self.fd)
			self.fd = os.open(self.filename, os.O_NONBLOCK | os.O_RDONLY)
			self.buf = ""
			return
		self.buf += data
		idx = self.buf.rindex('\n')  # cut off at last newline
		if idx > 0:
			cut = self.buf[:idx]
			self.buf = self.buf[idx:]
			self.inmessages.append(MAGIMessage(src=self.src, srcdock=self.srcdock,
											groups=self.dstgroups, docks=self.dstdocks,
											contenttype=MAGIMessage.TEXT, data=cut))

	def __repr__(self):
		return "TextPipe %d" % self.fd
	__str__ = __repr__

	def writable(self):
		""" Never writes """
		return False

	def handle_expt_event(self):
		log.warning("Exception on %s", self)
		self.handle_close()

