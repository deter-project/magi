#!/usr/bin/env python

import unittest2
import logging
import os
import pdb

from magi.tests.util import *
from magi.messaging.api import MAGIMessage
from magi.orchestrator.parse import AAL, AALParseError

class AALParseTest(unittest2.TestCase):
	"""
		Metaparse tests for AAL files. Assumes AAL is correct and 
		parses for correct structural references and AAL-specific 
		values that must be in the YAML for this to be a valid AAL.
	"""
	def test_triggerAgentParse(self):
		# This aal file is missing a reference when parsing the 
		# 'agent' argument in the trigger, so an AALParseError 
		# should be raised.
		self.assertRaises(AALParseError, AAL, os.path.join(os.path.dirname(__file__), 'triggerAgentTest_bad.aal'))

		# Everything is fine with this file. The trigger's agent field
		# should be gone and replaced with a nodes field.
		aal = AAL(os.path.join(os.path.dirname(__file__), 'triggerAgentTest_good.aal'))

		for stream in aal.aal['eventstreams']:
			for event in aal.aal['eventstreams'][stream]:
				if event['type'] == 'trigger':
					for trigger in event['triggers']:
						print '\nChecking trigger: %s' % trigger
						self.assertEquals('agent' in trigger, False)
						self.assertEquals('nodes' in trigger, True)

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


