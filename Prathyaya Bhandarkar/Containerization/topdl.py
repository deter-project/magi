#!/usr/local/bin/python

import re
import xml.parsers.expat
from xml.sax.saxutils import escape
from base64 import b64encode
from string import join

try:
    from fedid import fedid as fedid_class
except ImportError:
    class fedid_class:
	'''
	Dummy class to allow most of the topdl function when m2crypto and asn1
	are not around
	'''
	def __init__(self, hexstr=None):
	    if hexstr is not None: self.data = hexstr
	    else: self.data = "default data"

	def get_bits(self):
	    return self.data

class base:
    @staticmethod
    def init_class(c, arg):
	if isinstance(arg, dict):
	    try:
		return c(**arg)
	    except:
		print "%s" % arg
		raise
	elif isinstance(arg, c):
	    return arg
	else:
	    return None

    @staticmethod
    def make_list(a):
	if isinstance(a, basestring) or isinstance(a, dict): return [ a ]
	elif getattr(a, '__iter__', None): return a
	else: return [ a ]

    @staticmethod
    def init_string(s):
	"""
	Force a string coercion for everything but a None.
	"""
	if s is not None: return "%s" % s
	else: return None

    def remove_attribute(self, key):
	to_del = None
	attrs = getattr(self, 'attribute', [])
	for i, a in enumerate(attrs):
	    if a.attribute == key:
		to_del = i
		break
	
	if to_del: del attrs[i]

    def get_attribute(self, key):
	rv = None
	attrs = getattr(self, 'attribute', None)
	if attrs:
	    for a in attrs:
		if a.attribute == key:
		    rv = a.value
		    break
	return rv

    def set_attribute(self, key, value):
	attrs = getattr(self, 'attribute', None)
	if attrs is None:
	    return
	for a in attrs:
	    if a.attribute == key: 
		a.value = value
		break
	else:
	    attrs.append(Attribute(key, value))

class ConsistencyError(RuntimeError): pass
class NamespaceError(RuntimeError): pass

class Attribute(base):
    def __init__(self, attribute, value):
	self.attribute = self.init_string(attribute)
	self.value = self.init_string(value)

    def clone(self):
	return Attribute(attribute=self.attribute, value=self.value)

    def to_dict(self):
	return { 'attribute': self.attribute, 'value': self.value }
    def to_xml(self):
	return "<attribute>%s</attribute><value>%s</value>" % \
		(escape(self.attribute), escape(self.value))

class Capacity(base):
    def __init__(self, rate, kind):
	self.rate = float(rate)
	self.kind = self.init_string(kind)

    def clone(self):
	return Capacity(rate=self.rate, kind=self.kind)

    def to_dict(self):
	return { 'rate': float(self.rate), 'kind': self.kind }

    def to_xml(self):
	return "<rate>%f</rate><kind>%s</kind>" % (self.rate, self.kind)

class Latency(base):
    def __init__(self, time, kind):
	self.time = float(time)
	self.kind = self.init_string(kind)

    def clone(self):
	return Latency(time=self.time, kind=self.kind)

    def to_dict(self):
	return { 'time': float(self.time), 'kind': self.kind }

    def to_xml(self):
	return "<time>%f</time><kind>%s</kind>" % (self.time, self.kind)

class ServiceParam(base):
    def __init__(self, name, type):
	self.name = self.init_string(name)
	self.type = self.init_string(type)

    def clone(self):
	return ServiceParam(self.name. self.type)

    def to_dict(self):
	return { 'name': name, 'type': type }

    def to_xml(self):
	return "<name>%s</name><type>%s</type>" % (self.name, self.type)

class Service(base):
    def __init__(self, name, importer=[], param=[], description=None, 
	    status=None):
	self.name = self.init_string(name)
	self.importer = [self.init_string(i) \
		for i in self.make_list(importer)]
	self.param = [ self.init_class(ServiceParam, p) \
		for p in self.make_list(param) ]
	self.description = self.init_string(description)
	self.status = self.init_string(status)

    def clone(self):
	return Service(
		name=self.name, 
		importer=[ i for i in self.importer], 
		param=[p.clone() for p in self.param], 
		description=self.description,
		status=self.status)

    def to_dict(self):
	rv = { }
	if self.name is not None:
	    rv['name'] = self.name
	if self.importer:
	    rv['importer'] = [ i for i in self.importer ]
	if self.param:
	    rv['param'] = [ p.to_dict() for p in self.param ]
	if self.description is not None:
	    rv['description'] = self.description
	if self.status is not None:
	    rv['status'] = self.status
	return rv

    def to_xml(self):
	rv = '' 
	if self.name is not None:
	    rv += '<name>%s</name>' % self.name
	if self.importer:
	    rv += join(['<importer>%s</importer>' % i \
		    for i in self.importer],'')
	if self.param:
	    rv += join(['<param>%s</param>' % p.to_xml() \
		    for p in self.param], '')
	if self.description is not None:
	    rv += '<description>%s</description>' % self.description
	if self.status is not None:
	    rv += '<status>%s</status>' % self.status
	return rv

class Substrate(base):
    def __init__(self, name, capacity=None, latency=None, attribute=[],
	    localname=[], status=None, service=[], operation=[]):
	self.name = self.init_string(name)
	self.capacity = self.init_class(Capacity, capacity)
	self.latency = self.init_class(Latency, latency)
	self.attribute = [ self.init_class(Attribute, a) \
		for a in self.make_list(attribute) ]
	self.localname = [ self.init_string(ln)\
		for ln in self.make_list(localname) ]
	self.status = self.init_string(status)
	self.service = [ self.init_class(Service, s) \
		for s in self.make_list(service)]
	self.operation = [self.init_string(op) \
		for op in self.make_list(operation)]
	self.interfaces = [ ]

    def clone(self):
	if self.capacity: c = self.capacity.clone()
	else: c = None

	if self.latency: l = self.latency.clone()
	else: l = None

	return Substrate(name=self.name,
		capacity=c,
		latency=l,
		attribute = [a.clone() for a in self.attribute],
		localname = [ ln for ln in self.localname],
		status = self.status,
		service = [ s.clone() for s in self.service],
		operation=[ op for op in self.operation])

    def to_dict(self):
	rv = { 'name': self.name }
	if self.capacity:
	    rv['capacity'] = self.capacity.to_dict()
	if self.latency:
	    rv['latency'] = self.latency.to_dict()
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	if self.localname:
	    rv['localname'] = [ ln for ln in self.localname ]
	if self.status:
	    rv['status'] = self.status
	if self.service:
	    rv['service'] = [s.to_dict() for s in self.service]
	if self.operation:
	    rv['operation'] = [op for op in self.operation]
	return rv

    def to_xml(self):
	rv = "<name>%s</name>" % escape(self.name)
	if self.capacity is not None:
	    rv += "<capacity>%s</capacity>" % self.capacity.to_xml()
	if self.latency is not None:
	    rv += "<latency>%s</latency>" % self.latency.to_xml()
	
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	if self.localname:
	    rv += join(['<localname>%s</localname>' % ln \
		    for ln in self.localname], '')
	if self.status is not None:
	    rv += '<status>%s</status>' % self.status
	if self.service:
	    rv += join(['<service>%s</service' % s.to_xml() \
		    for s in self.service], '')
	if self.operation:
	    rv += join(['<operation>%s</operation>' % op \
		    for op in self.operation], '')
	return rv

    def __getstate__(self):
	d = self.__dict__.copy()
	del d['interfaces']
	return d

    def __setstate__(self, d):
	# Import everything from the pickle dict (except what we excluded in
	# __getstate__)
	self.__dict__.update(d)
	self.interfaces = []

class CPU(base):
    def __init__(self, type, attribute=[]):
	self.type = self.init_string(type)
	self.attribute = [ self.init_class(Attribute, a) for a in \
		self.make_list(attribute) ]

    def clone(self):
	return CPU(type=self.type,
		attribute = [a.clone() for a in self.attribute])

    def to_dict(self):
	rv = { 'type': self.type}
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return rv

    def to_xml(self):
	rv = "<type>%s</type>" % escape(self.type)
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return rv


class Storage(base):
    def __init__(self, amount, persistence, attribute=[]):
	self.amount = float(amount)
	self.persistence = self.init_string(persistence)
	self.attribute = [ self.init_class(Attribute, a) \
		for a in self.make_list(attribute) ]

    def clone(self):
	return Storage(amount=self.amount, persistence=self.persistence, 
		attribute = [a.clone() for a in self.attribute])

    def to_dict(self):
	rv = { 'amount': float(self.amount), 'persistence': self.persistence }
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return rv

    def to_xml(self):
	rv = "<amount>%f</amount><persistence>%s</persistence>" % \
		(self.amount, escape(self.persistence))
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return rv


class OperatingSystem(base):
    def __init__(self, name=None, version=None, distribution=None,
	    distributionversion=None, attribute=[]):
	self.name = self.init_string(name)
	self.version = self.init_string(version)
	self.distribution = self.init_string(distribution)
	self.distributionversion = self.init_string(distributionversion)
	self.attribute = [ self.init_class(Attribute, a) \
		for a in self.make_list(attribute) ]

    def clone(self):
	return OperatingSystem(name=self.name,
		version=self.version,
		distribution=self.distribution,
		distributionversion=self.distributionversion,
		attribute = [ a.clone() for a in self.attribute])

    def to_dict(self):
	rv = { }
	if self.name: rv['name'] = self.name
	if self.version: rv['version'] = self.version
	if self.distribution: rv['distribution'] = self.distribution
	if self.distributionversion: 
	    rv['distributionversion'] = self.distributionversion
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return rv

    def to_xml(self):
	rv = ""
	if self.name: rv += "<name>%s</name>" % escape(self.name)
	if self.version: rv += "<version>%s</version>" % escape(self.version)
	if self.distribution: 
	    rv += "<distribution>%s</distribution>" % escape(self.distribution)
	if self.distributionversion: 
	    rv += "<distributionversion>%s</distributionversion>" % \
		    escape(self.distributionversion)
	
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return rv


class Software(base):
    def __init__(self, location, install=None, attribute=[]):
	self.location = self.init_string(location)
	self.install = self.init_string(install)
	self.attribute = [ self.init_class(Attribute, a)\
		for a in self.make_list(attribute) ]

    def clone(self):
	return Software(location=self.location, install=self.install, 
		attribute=[a.clone() for a in self.attribute])

    def to_dict(self):
	rv = { 'location': self.location }
	if self.install: rv['install'] = self.install
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return rv

    def to_xml(self):
	rv = "<location>%s</location>" % escape(self.location)
	if self.install: rv += "<install>%s</install>" % self.install
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return rv


class Interface(base):
    def __init__(self, substrate, name=None, capacity=None, latency=None,
	    attribute=[], element=None):
	self.name = self.init_string(name)

	self.substrate = self.make_list(substrate)
	self.capacity = self.init_class(Capacity, capacity)
	self.latency = self.init_class(Latency, latency)
	self.attribute = [ self.init_class(Attribute, a) \
		for a in self.make_list(attribute) ]
	self.element = element 
	self.subs = [ ]

    def clone(self):
	if self.capacity: c = self.capacity.clone()
	else: c = None

	if self.latency: l = self.latency.clone()
	else: l = None

	return Interface(substrate=[s for s in self.substrate], name=self.name,
		capacity=c, latency=l,
		attribute = [ a.clone() for a in self.attribute])

    def to_dict(self):
	rv = { 'substrate': self.substrate, 'name': self.name }
	if self.capacity:
	    rv['capacity'] = self.capacity.to_dict()
	if self.latency:
	    rv['latency'] = self.latency.to_dict()
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return rv

    def to_xml(self):
	rv = join(["<substrate>%s</substrate>" % escape(s) \
		for s in self.substrate], "")
	rv += "<name>%s</name>" % self.name
	if self.capacity:
	    rv += "<capacity>%s</capacity>" % self.capacity.to_xml()
	if self.latency:
	    rv += "<latency>%s</latency>" % self.latency.to_xml()
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return rv

    def __getstate__(self):
	d = self.__dict__.copy()
	del d['subs']
	return d

    def __setstate__(self, d):
	# Import everything from the pickle dict (except what we excluded in
	# __getstate__)
	self.__dict__.update(d)
	self.subs = []


class ID(base):
    def __init__(self, fedid=None, uuid=None, uri=None, localname=None,
	    kerberosUsername=None):
	self.fedid=fedid_class(hexstr="%s" % fedid)
	self.uuid = self.init_string(uuid)
	self.uri = self.init_string(uri)
	self.localname =self.init_string( localname)
	self.kerberosUsername = self.init_string(kerberosUsername)

    def clone(self):
	return ID(self.fedid, self.uuid, self.uri, self.localname,
		self.kerberosUsername)

    def to_dict(self):
	rv = { }
	if self.fedid: rv['fedid'] = self.fedid
	if self.uuid: rv['uuid'] = self.uuid
	if self.uri: rv['uri'] = self.uri
	if self.localname: rv['localname'] = self.localname
	if self.kerberosUsername: rv['kerberosUsername'] = self.kerberosUsername
	return rv

    def to_xml(self):
	if self.uuid: rv = "<uuid>%s</uuid>" % b64encode(self.uuid)
	elif self.fedid: rv = "<fedid>%s</fedid>" % \
		b64encode(self.fedid.get_bits())
	elif self.uri: rv = "<uri>%s</uri>" % escape(self.uri)
	elif self.localname: 
	    rv = "<localname>%s</localname>" % escape(self.localname)
	elif self.kerberosUsername: 
	    rv = "<kerberosUsername>%s</kerberosUsername>" % \
		    escape(self.kerberosUsername)
	return rv

class Computer(base):
    def __init__(self, name, cpu=[], os=[], software=[], storage=[],
	    interface=[], attribute=[], localname=[], status=None, service=[],
	    operation=[]):
	def assign_element(i):
	    i.element = self

	self.name = self.init_string(name)
	self.cpu = [ self.init_class(CPU, c)  for c in self.make_list(cpu) ]
	self.os = [ self.init_class(OperatingSystem, c) \
		for c in self.make_list(os) ]
	self.software = [ self.init_class(Software, c) \
		for c in self.make_list(software) ]
	self.storage = [ self.init_class(Storage, c) \
		for c in self.make_list(storage) ]
	self.interface = [ self.init_class(Interface, c) \
		for c in self.make_list(interface) ]
	self.attribute = [ self.init_class(Attribute, a) \
		for a in self.make_list(attribute) ]
	self.localname = [ self.init_string(ln)\
		for ln in self.make_list(localname) ]
	self.status = self.init_string(status)
	self.service = [ self.init_class(Service, s) \
		for s in self.make_list(service)]
	self.operation = [self.init_string(op) \
		for op in self.make_list(operation)]
	map(assign_element, self.interface)

    def clone(self):
	# Copy the list of names
	return Computer(name=self.name,
		cpu=[x.clone() for x in self.cpu],
		os=[x.clone() for x in self.os],
		software=[x.clone() for x in self.software],
		storage=[x.clone() for x in self.storage],
		interface=[x.clone() for x in self.interface],
		attribute=[x.clone() for x in self.attribute],
		localname =[ ln for ln in self.localname],
		status = self.status,
		service = [s.clone() for s in self.service],
		operation = [op for op in self.operation])

    def to_dict(self):
	rv = { }
	if self.name:
	    rv['name'] = self.name
	if self.cpu:
	    rv['cpu'] = [ c.to_dict() for  c in self.cpu ]
	if self.os:
	    rv['os'] = [ o.to_dict() for o in self.os ]
	if self.software:
	    rv['software'] = [ s.to_dict() for s in self.software ]
	if self.storage:
	    rv['storage'] = [ s.to_dict() for s in self.storage ]
	if self.interface:
	    rv['interface'] = [ i.to_dict() for i in self.interface ]
	if self.attribute:
	    rv['attribute'] = [ i.to_dict() for i in self.attribute ]
	if self.localname:
	    rv['localname'] = [ ln for ln in self.localname ]
	if self.status:
	    rv['status'] = self.status
	if self.service:
	    rv['service'] = [s.to_dict() for s in self.service]
	if self.operation:
	    rv['operation'] = [op for op in self.operation]
	return { 'computer': rv }

    def to_xml(self):
	rv = "<name>%s</name>" % escape(self.name)
	if self.cpu:
	    rv += join(["<cpu>%s</cpu>" % c.to_xml() for c in self.cpu], "")
	if self.os:
	    rv += join(["<os>%s</os>" % o.to_xml() for o in self.os], "")
	if self.software:
	    rv += join(["<software>%s</software>" % s.to_xml() \
		    for s in self.software], "")
	if self.storage:
	    rv += join(["<storage>%s</storage>" % s.to_xml() \
		    for s in self.storage], "")
	if self.interface:
	    rv += join(["<interface>%s</interface>" % i.to_xml() 
		for i in self.interface], "")
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	if self.localname:
	    rv += join(['<localname>%s</localname>' % ln \
		    for ln in self.localname], '')
	if self.status is not None:
	    rv += '<status>%s</status>' % self.status
	if self.service:
	    rv += join(['<service>%s</service' % s.to_xml() \
		    for s in self.service], '')
	if self.operation:
	    rv += join(['<operation>%s</operation>' % op \
		    for op in self.operation], '')
	return "<computer>%s</computer>" % rv



class Testbed(base):
    def __init__(self, uri, type, interface=[], attribute=[], localname=[],
	    status=None, service=[], operation=[]):
	self.uri = self.init_string(uri)
	self.type = self.init_string(type)
	self.interface = [ self.init_class(Interface, c) \
		for c in self.make_list(interface) ]
	self.attribute = [ self.init_class(Attribute, c) \
		for c in self.make_list(attribute) ]
	self.localname = [ self.init_string(ln)\
		for ln in self.make_list(localname) ]
	self.status = self.init_string(status)
	self.service = [ self.init_class(Service, s) \
		for s in self.make_list(service)]
	self.operation = [self.init_string(op) \
		for op in self.make_list(operation)]

    def clone(self):
	return Testbed(self.uri, self.type,
		interface=[i.clone() for i in self.interface],
		attribute=[a.clone() for a in self.attribute],
		localname = [ ln for ln in self.localname ],
		status=self.status,
		service=[s.clone() for s in self.service ],
		operation = [ op for op in self.operation ])

    def to_dict(self):
	rv = { }
	if self.uri: rv['uri'] = self.uri
	if self.type: rv['type'] = self.type
	if self.interface:
	    rv['interface'] = [ i.to_dict() for i in self.interface]
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute]
	if self.localname:
	    rv['localname'] = [ ln for ln in self.localname ]
	if self.status:
	    rv['status'] = self.status
	if self.service:
	    rv['service'] = [s.to_dict() for s in self.service]
	if self.operation:
	    rv['operation'] = [op for op in self.operation]
	return { 'testbed': rv }

    def to_xml(self):
	rv = "<uri>%s</uri><type>%s</type>" % \
		(escape(self.uri), escape(self.type))
	if self.interface:
	    rv += join(["<interface>%s</interface>" % i.to_xml() 
		for i in self.interface], "")
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	if self.localname:
	    rv += join(['<localname>%s</localname>' % ln \
		    for ln in self.localname], '')
	if self.status is not None:
	    rv += '<status>%s</status>' % self.status
	if self.service:
	    rv += join(['<service>%s</service' % s.to_xml() \
		    for s in self.service], '')
	if self.operation:
	    rv += join(['<operation>%s</operation>' % op \
		    for op in self.operation], '')
	return "<testbed>%s</testbed>" % rv

	

class Segment(base):
    def __init__(self, id, type, uri, interface=[], attribute=[]):
	self.id = self.init_class(ID, id)
	self.type = self.init_string(type)
	self.uri = self.init_string(uri)
	self.interface = [ self.init_class(Interface, c) \
		for c in self.make_list(interface) ]
	self.attribute = [ self.init_class(Attribute, c) \
		for c in self.make_list(attribute) ]

    def clone(self):
	return Segment(self.id.clone(), self.type, self.uri, 
		interface=[i.clone() for i in self.interface], 
		attribute=[a.clone() for a in self.attribute])

    def to_dict(self):
	rv = { }
	if self.id: rv['id'] = self.id.to_dict()
	if self.type: rv['type'] = self.type
	if self.uri: rv['uri'] = self.uri
	if self.interface:
	    rv['interface'] = [ i.to_dict() for i in self.interface ]
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return { 'segment': rv }

    def to_xml(self):
	rv = "<id>%s</id><uri>%s</uri><type>%s</type>" % \
		(self.id.to_xml(), escape(self.uri), escape(self.type))
	if self.interface:
	    rv += join(["<interface>%s</interface>" % i.to_xml() 
		for i in self.interface], "")
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return "<segment>%s</segment>" % rv

class Other(base):
    def __init__(self, interface=[], attribute=[]):
	self.interface = [ self.init_class(Interface, c) \
		for c in self.make_list(interface) ]
	self.attribute = [ self.init_class(Attribute, c) \
		for c in self.make_list(attribute) ]

    def clone(self):
	return Other(interface=[i.clone() for i in self.interface], 
		attribute=[a.clone() for a in attribute])

    def to_dict(self):
	rv = {}
	if self.interface:
	    rv['interface'] = [ i.to_dict() for i in self.interface ]
	if self.attribute:
	    rv['attribute'] = [ a.to_dict() for a in self.attribute ]
	return {'other': rv }

    def to_xml(self):
	rv = ""
	if self.interface:
	    rv += join(["<interface>%s</interface>" % i.to_xml() 
		for i in self.interface], "")
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return "<other>%s</other>" % rv

class Topology(base):
    version = "1.0"
    @staticmethod
    def init_element(e):
	"""
	e should be of the form { typename: args } where args is a dict full of
	the right parameters to initialize the element.  e should have only one
	key, but we walk e's keys in an arbitrary order and instantiate the
	first key we know how to.
	"""
	classmap = {
		'computer': Computer,
		'testbed': Testbed,
		'segment': Segment,
		'other': Other,
	    }

	if isinstance(e, dict):
	    for k in e.keys():
		cl = classmap.get(k, None)
		if cl: return cl(**e[k])
	else:
	    return e

    def __init__(self, substrates=[], elements=[], attribute=[], 
	    version=None):

	if version is None: self.version = Topology.version
	else: self.version = version

	self.substrates = [ self.init_class(Substrate, s) \
		for s in self.make_list(substrates) ]
	self.elements = [ self.init_element(e) \
		for e in self.make_list(elements) ]
	self.attribute = [ self.init_class(Attribute, c) \
		for c in self.make_list(attribute) ]
	self.incorporate_elements()

    @staticmethod
    def name_element_interfaces(e):
	names = set([i.name for i in e.interface if i.name])
	inum = 0
	for i in [ i for i in e.interface if not i.name]:
	    while inum < 1000:
		n = "inf%03d" % inum
		inum += 1
		if n not in names:
		    i.name = n
		    break
	    else:
		raise NamespaceError("Cannot make new interface name")



    def name_interfaces(self):
	"""
	For any interface without a name attribute, assign a unique one within
	its element.
	"""

	for e in self.elements:
	    self.name_element_interfaces(e)


    def incorporate_elements(self):

	# Could to this init in one gulp, but we want to look for duplicate
	# substrate names
	substrate_map = { }
	for s in self.substrates:
	    s.interfaces = [ ]
	    if not substrate_map.has_key(s.name):
		substrate_map[s.name] = s
	    else:
		raise ConsistencyError("Duplicate substrate name %s" % s.name)

	for e in self.elements:
	    self.name_element_interfaces(e)
	    for i in e.interface:
		i.element = e
		i.subs = [ ]
		for sn in i.substrate:
		    # NB, interfaces have substrate names in their substrate
		    # attribute.
		    if substrate_map.has_key(sn):
			sub = substrate_map[sn]
			i.subs.append(sub)
			sub.interfaces.append(i)
		    else:
			raise ConsistencyError("No such substrate for %s" % sn)

    def clone(self):
	return Topology(substrates=[s.clone() for s in self.substrates], 
		elements=[e.clone() for e in self.elements],
		attribute=[a.clone() for a in self.attribute],
		version=self.version)


    def make_indices(self):
	sub_index = dict([(s.name, s) for s in self.substrates])
	elem_index = dict([(n, e) for e in self.elements for n in e.name])

    def to_dict(self):
	rv = { }
	rv['version'] = self.version
	if self.substrates:
	    rv['substrates'] = [ s.to_dict() for s in self.substrates ]
	if self.elements:
	    rv['elements'] = [ s.to_dict() for s in self.elements ]
	if self.attribute:
	    rv['attribute'] = [ s.to_dict() for s in self.attribute]
	return rv

    def to_xml(self):
	rv = "<version>%s</version>" % escape(self.version)
	if self.substrates:
	    rv += join(["<substrates>%s</substrates>" % s.to_xml() \
		    for s in self.substrates], "")
	if self.elements:
	    rv += join(["<elements>%s</elements>" % e.to_xml() \
		    for e in self.elements], "")
	if self.attribute:
	    rv += join(["<attribute>%s</attribute>" % a.to_xml() \
		    for a in self.attribute], "")
	return rv

    def __setstate__(self, d):
	# Import everything from the pickle dict and call incorporate to
	# connect them.
	self.__dict__.update(d)
	self.incorporate_elements()


def topology_from_xml(string=None, file=None, filename=None, top="topology"):
    class parser:
	def __init__(self, top):
	    self.stack = [ ]
	    self.chars = ""
	    self.key = ""
	    self.have_chars = False
	    self.current = { }
	    self.in_cdata = False
	    self.in_top = False
	    self.top = top
	
	def start_element(self, name, attrs):
	    self.chars = ""
	    self.have_chars = False
	    self.key = str(name)

	    if name == self.top:
		self.in_top = True

	    if self.in_top:
		self.stack.append((self.current, self.key))
		self.current = { }

	def end_element(self, name):
	    if self.in_top:
		if self.have_chars:
		    self.chars = self.chars.strip()
		    if len(self.chars) >0:
			addit = self.chars
		    else:
			addit = self.current
		else:
		    addit = self.current

		parent, key = self.stack.pop()
		if parent.has_key(key):
		    if isinstance(parent[key], list):
			parent[key].append(addit)
		    else:
			parent[key] = [parent[key], addit]
		else:
		    parent[key] = addit
		self.current = parent
		self.key = key

	    self.chars = ""
	    self.have_chars = False

	    if name == self.top:
		self.in_top= False

	def char_data(self, data):
	    if self.in_top:
		self.have_chars = True
		self.chars += data

    p = parser(top=top)
    xp = xml.parsers.expat.ParserCreate()

    xp.StartElementHandler = p.start_element
    xp.EndElementHandler = p.end_element
    xp.CharacterDataHandler = p.char_data

    num_set = len([ x for x in (string, filename, file)\
	    if x is not None ])

    if num_set != 1:
	raise RuntimeError("Exactly one one of file, filename and string " + \
		"must be set")
    elif filename:
	f = open(filename, "r")
	xp.ParseFile(f)
	f.close()
    elif file:
	xp.ParseFile(file)
    elif string:
	xp.Parse(string, True)
    else:
	return None

    return Topology(**p.current[top])

def topology_from_startsegment(req):
    """
    Generate a topology from a StartSegment request to an access controller.
    This is a little helper to avoid some gross looking syntax.  It accepts
    either a request enclosed in the StartSegmentRequestBody, or one with that
    outer dict removed.
    """

    if 'StartSegmentRequestBody' in req: r = req['StartSegmentRequestBody']
    else: r = req
    
    if 'segmentdescription' in r and \
	    'topdldescription' in r['segmentdescription']:
	return Topology(**r['segmentdescription']['topdldescription'])
    else:
	return None

def topology_to_xml(t, top=None):
    """
    Print the topology as XML, recursively using the internal classes to_xml()
    methods.
    """

    if top: return "<%s>%s</%s>" % (top, t.to_xml(), top)
    else: return t.to_xml()

def topology_to_vtopo(t):
    nodes = [ ]
    lans = [ ]

    for eidx, e in enumerate(t.elements):
	if isinstance(e, Computer):
	    if e.name: name = e.name
	    else: name = "unnamed_node%d" % eidx
	    
	    ips = [ ]
	    for idx, i in enumerate(e.interface):
		ip = i.get_attribute('ip4_address')
		ips.append(ip)
		port = "%s:%d" % (name, idx)
		for idx, s in enumerate(i.subs):
		    bw = 100000
		    delay = 0.0
		    if s.capacity:
			bw = s.capacity.rate
		    if i.capacity:
			bw = i.capacity.rate

		    if s.latency:
			delay = s.latency.time
		    if i.latency:
			bw = i.latency.time

		    lans.append({
			'member': port,
			'vname': s.name,
			'ip': ip,
			'vnode': name,
			'delay': delay,
			'bandwidth': bw,
			})
	    nodes.append({
		'ips': ":".join(ips),
		'vname': name,
		})

    return { 'node': nodes, 'lan': lans }

def to_tcl_name(n):
    t = re.sub('-(\d+)', '(\\1)', n)
    return t

def generate_portal_command_filter(cmd, add_filter=None, suffix=''):
    def rv(e):
	s =""
	if isinstance(e, Computer):
	    gw = e.get_attribute('portal')
	    if add_filter and callable(add_filter):
		add = add_filter(e)
	    else:
		add = True
	    if gw and add:
		s = "%s ${%s} %s\n" % (cmd, to_tcl_name(e.name), suffix)
	return s
    return rv

def generate_portal_image_filter(image):
    def rv(e):
	s =""
	if isinstance(e, Computer):
	    gw = e.get_attribute('portal')
	    if gw:
		s = "tb-set-node-os ${%s} %s\n" % (to_tcl_name(e.name), image)
	return s
    return rv

def generate_portal_hardware_filter(type):
    def rv(e):
	s =""
	if isinstance(e, Computer):
	    gw = e.get_attribute('portal')
	    if gw:
		s = "tb-set-hardware ${%s} %s\n" % (to_tcl_name(e.name), type)
	return s
    return rv


def topology_to_ns2(t, filters=[], routing="Manual"):
    out = """
set ns [new Simulator]
source tb_compat.tcl

"""

    for e in t.elements:
	rpms = ""
	tarfiles = ""
	if isinstance(e, Computer):
	    name = to_tcl_name(e.name)
	    out += "set %s [$ns node]\n" % name
	    if e.os and len(e.os) == 1:
		osid = e.os[0].get_attribute('osid')
		if osid:
		    out += "tb-set-node-os ${%s} %s\n" % (name, osid)
	    hw = e.get_attribute('type')
	    if hw:
		out += "tb-set-hardware ${%s} %s\n" % (name, hw)
	    for s in e.software:
		if s.install:
		    tarfiles += "%s %s " % (s.install, s.location)
		else:
		    rpms += "%s " % s.location
	    if rpms:
		out += "tb-set-node-rpms ${%s} %s\n" % (name, rpms)
	    if tarfiles:
		out += "tb-set-node-tarfiles ${%s} %s\n" % (name, tarfiles)
	    startcmd = e.get_attribute('startup')
	    if startcmd:
		out+= 'tb-set-node-startcmd ${%s} "%s"\n' % (name, startcmd)
	    for f in filters:
		out += f(e)
	    out+= "\n"
    
    for idx, s in enumerate(t.substrates):
	if len(s.interfaces) < 2: continue
	loss = s.get_attribute('loss')
	if s.latency: delay = s.latency.time
	else: delay = 0

	if s.capacity: rate = s.capacity.rate
	else: rate = 100000
	name = to_tcl_name(s.name or "sub%d" % idx)

	# Lan
	members = [ to_tcl_name("${%s}") % i.element.name \
		for i in s.interfaces]
	out += 'set %s [$ns make-lan "%s" %fkb %fms ]\n' % \
		(name, " ".join([to_tcl_name(m) for m in members]),
			rate, 2 * delay)
	if loss:
	    "tb-set-lan-loss ${%s} %f\n" % (name, float(loss))

	for i in s.interfaces:
	    e = i.element
	    ip = i.get_attribute("ip4_address")
	    if ip:
		out += "tb-set-ip-lan ${%s} ${%s} %s\n" % \
			(to_tcl_name(e.name), name, ip)
	    if i.capacity :
		out += "tb-set-node-lan-bandwidth ${%s} ${%s} %fkb\n" % \
			(to_tcl_name(e.name), name, i.capacity.rate)
	    if i.latency :
		out += "tb-set-node-lan-delay ${%s} ${%s} %fms\n" % \
			(to_tcl_name(e.name), name, i.latency.time)
	    iloss = i.get_attribute('loss')
	    if loss and iloss != loss :
		out += "tb-set-node-lan-loss ${%s} ${%s} %f\n" % \
			(to_tcl_name(e.name), name, float(loss))
	out+= "\n"
	for f in filters:
	    out+= f(s)
    out+="$ns rtproto %s" % routing
    out+="""
$ns run
"""
    return out

def topology_to_rspec(t, filters=[]):
    out = '<?xml version="1.0" encoding="UTF-8"?>\n' + \
	'<rspec xmlns="http://www.protogeni.net/resources/rspec/0.1"\n' + \
	'\txmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n' + \
	'\txsi:schemaLocation="http://www.protogeni.net/resources/rspec/0.1 '+ \
	'http://www.protogeni.net/resources/rspec/0.1/request.xsd"\n' + \
	'\ttype="request" >\n'

    ifname = { }
    ifnode = { }

    for e in [e for e in t.elements if isinstance(e, Computer)]:
	name = e.name
	virt_type = e.get_attribute("virtualization_type") or "emulab-vnode"
	exclusive = e.get_attribute("exclusive") or "1"
	hw = e.get_attribute("type") or "pc";
	slots = e.get_attribute("slots") or "1";
	startup = e.get_attribute("startup")

	extras = ""
	if startup: extras += '\t\tstartup_command="%s"\n' % startup
	out += '\t<node virtual_id="%s"\n\t\tvirtualization_type="%s"\n' % \
		(name, virt_type)
	out += '\t\texclusive="%s"' % exclusive
	if extras: out += '\n%s' % extras
	out += '>\n'
	out += '\t\t<node_type type_name="%s" slots="%s"/>\n' % (hw, slots)
	for i, ii in enumerate(e.interface):
	    out += '\t\t<interface virtual_id="%s"/>\n' % ii.name
	    ifnode[ii] = name
	for f in filters:
	    out += f(e)
	out += '\t</node>\n'

    for i, s in enumerate(t.substrates):
	if len(s.interfaces) == 0: 
	    continue
	out += '\t<link virtual_id="%s" link_type="ethernet">\n' % s.name
	if s.capacity and s.capacity.kind == "max":
	    bwout = True
	    out += '\t\t<bandwidth>%d</bandwidth>\n' % s.capacity.rate
	else:
	    bwout = False
	if s.latency and s.latency.kind == "max":
	    out += '\t\t<latency>%d</latency>\n' % s.latency.time
	elif bwout:
	    out += '\t\t<latency>0</latency>\n'
	for ii in s.interfaces:
	    out += ('\t\t<interface_ref virtual_node_id="%s" ' + \
		    'virtual_interface_id="%s"/>\n') % (ifnode[ii], ii.name)
	for f in filters:
	    out += f(s)
	out += '\t</link>\n'
    out += '</rspec>\n'
    return out

