# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

""" 
This file is here to simply names and provide a way to import just
what we want into a file with a from X import *
"""

import random

random.seed()

def capfunc(val, cap):
	if cap is not None and val > cap: return cap
	return val

def minmax(min, max):
	return random.uniform(min, max)

def gamma(alpha, rate, cap = None):
	return capfunc(random.gammavariate(alpha, rate), cap)

def pareto(alpha, scale = 1.0, cap = None):
	return capfunc(random.paretovariate(alpha) * scale, cap)

def expo(lambd, scale = 1.0, cap = None):
	return capfunc(random.expovariate(lambd) * scale, cap)


