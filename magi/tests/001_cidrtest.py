#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import unittest2
from magi.util.cidr import CIDR

class CidrTest(unittest2.TestCase):

	""" CIDR represents a range of addresses based on a base and mask bits """

	def testInputStr(self):
		""" Test CIDR built from input str """
		cidr = CIDR(inputstr="1.2.3.4/255.255.255.0")
		self.assertEqual(cidr.basestr, "1.2.3.0")
		self.assertEqual(cidr.maskstr, "255.255.255.0")
		self.assertEqual(cidr.base, 16909056)
		self.assertEqual(cidr.mask, 0xFFFFFF00)
		self.assertEqual(cidr.maskbits, 24)
		self.assertEqual(cidr.range, 256)

	def testArgStr(self):
		""" Test CIDR built from base and mask str """
		cidr = CIDR(basestr="1.2.3.4", maskstr="255.255.255.0")
		self.assertEqual(cidr.basestr, "1.2.3.0")
		self.assertEqual(cidr.maskstr, "255.255.255.0")
		self.assertEqual(cidr.base, 16909056)
		self.assertEqual(cidr.mask, 0xFFFFFF00)
		self.assertEqual(cidr.maskbits, 24)
		self.assertEqual(cidr.range, 256)


