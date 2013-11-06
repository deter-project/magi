
import unittest2
from magi.messaging.magimessage import MAGIMessage, DefaultCodec

class MAGIMessageTest(unittest2.TestCase):
	"""
		Testing of basics in MAGIMessage class
	"""

	def test_encodeAndDecode(self):
		""" Test encoding and decoding of a message header """
		msg = MAGIMessage()
		msg.msgid = 1234
		msg.flags = 0x63
		msg.contenttype = MAGIMessage.YAML
		msg.src = "mynode"
		msg.srcdock = "sourcedock"
		msg.hmac = "123456789"
		msg.dstnodes = set(['n1', 'n2'])
		msg.dstgroups = set(['g1', 'g2'])
		msg.dstdocks = set(['d1', 'd2'])
		msg.sequence = 98765
		msg.timestamp = 12347890
		msg.data = None

		codec = DefaultCodec()

		hdr = codec.encode(msg)
		ret, hdrsize = codec.decode(hdr)
		ret.data = None

		for k, v in msg.__dict__.iteritems():
			if k[0] == '_':
				continue
			self.assertEquals(getattr(ret, k), v)


