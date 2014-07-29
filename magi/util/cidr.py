#!/usr/bin/python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import random
import struct
import socket

class NextHop(object):
	""" Simple tuple like deal for hold next hop information for routing classes """
	def __init__(self, cidr, hop):
		self.cidr = cidr
		self.hop = hop

	def __repr__(self):
		return "%s/%s - %s" % (self.cidr.basestr, self.cidr.maskstr, self.hop)


class CIDR(object):
	""" CIDR represents a range of addresses based on a base and mask bits """

	def __init__(self, **args):
		""" Initialize the CIDR with either 'addr/mask', just 'addr' or as separate args """ 
		if ('inputstr' in args):
			p = args['inputstr'].split('/')
			self.basestr = p[0]
			if (len(p) > 1) and (len(p[1]) > 0):
				self.maskstr = p[1]
			else:
				self.maskstr = "255.255.255.255"
		else:
			self.basestr = args['basestr']
			self.maskstr = args['maskstr']

		if ('.' in self.maskstr):
			self.mask = self.str2int(self.maskstr)
			self.maskbits = self.int2bits(self.mask)
		else:
			self.maskbits = int(self.maskstr)
			self.mask = self.bits2int(self.maskbits)
			self.maskstr = self.int2str(self.mask) 

		self.range = pow(2, (32-self.maskbits))
		self.base = self.str2int(self.basestr)
		self.base = self.base & self.mask
		self.basestr = self.int2str(self.base) 


	def randomAddress(self):
		return self.int2str(long(self.base + (random.random() * self.range)))

	def __contains__(self, addrstr):
		""" Provides 'in' operator functionality for CIDR """
		addr = self.str2int(addrstr)
		return addr & self.mask == self.base
		
	def __cmp__(self, other):
		""" Used to test if two CIDRs are the same or not, less/greater is not really useful """
		if not other:  # cmp to None "successfully"
			return 1
		if self.mask == other.mask:
			if self.base == other.base: return 0
			if self.base < other.base: return -1
			if self.base > other.base: return 1
		if self.mask < other.mask: return -1
		if self.mask > other.mask: return 1
		return 1

	def __hash__(self):
		return hash(self.base) + hash(self.mask)
		
	# Pythons ntohl/htonl is wonky, returns a signed value, so I wrote this one
	def str2int(self, inputstr):
		a = struct.unpack('4B', socket.inet_aton(inputstr))
		return (a[0] * pow(2,24)) + (a[1] * pow(2,16)) + (a[2] * pow(2,8)) + a[3];

	def int2str(self, input):
		return socket.inet_ntoa(struct.pack('4B', input>>24, input>>16&0xFF, input>>8&0xFF, input&0xFF))

	# Count the number of bits in a mask value
	def int2bits(self, address):
		for idx in range(31, -1, -1):  # 31 to 0 inclusive
			if (address & (1<<idx)) == 0:
				return 31 - idx
		return 32

	# Count the number of bits in a mask value
	def bits2int(self, bitcount):
		address = 0
		for idx in range(31, (31-bitcount), -1):  # 31 to X inclusive
			address |= (1<<idx)
		return address

	def __repr__(self):
		return "%s/%s" % (self.basestr, self.maskstr)


