#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import unittest2
from magi.util.distributions import *

class DistTest(unittest2.TestCase):

	def testMinMax(self):
		""" Test MinMax for execution and proper range """
		for ii in range(200):
			val = minmax(1, 100)
			self.assertTrue(val >= 1.0 and val <= 100.0)

	def testGamma(self):
		""" Test Gamma for execution and proper range """
		for ii in range(200):
			val = gamma(1.2, 1.4, 100)
			self.assertTrue(val > 0.0 and val <= 100.0)

	def testPareto(self):
		""" Test Pareto for execution and proper range """
		for ii in range(200):
			val = pareto(1.2, 1.4, 100)
			self.assertTrue(val > 0.0 and val <= 100.0)

	def testExpo(self):
		""" Test Expo for execution and proper range """
		for ii in range(200):
			val = expo(1.2, 1.4, 100)
			self.assertTrue(val > 0.0 and val <= 100.0)

