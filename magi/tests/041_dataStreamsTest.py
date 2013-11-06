#!/usr/bin/env python

import unittest2
import logging

class DataStreamsTest(unittest2.TestCase):
	"""
		Testing of daemon and some of its components
	"""

	#def setUp(self):

	#def tearDown(self):

	#def test_MemoryStorage(self):
	# Don't do rotoating memory anymore so nothing to test

if __name__ == '__main__':
	import signal
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)

