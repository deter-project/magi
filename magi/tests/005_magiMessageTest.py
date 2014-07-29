#!/usr/bin/env python

import unittest2
import logging
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
		
		self.assertEquals(hdrsize, len(hdr))

		for k, v in msg.__dict__.iteritems():
			if k[0] == '_':
				continue
			self.assertEquals(getattr(ret, k), v)


if __name__ == '__main__':
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.INFO)
	unittest2.main(verbosity=2)
