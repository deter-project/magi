#!/usr/bin/env python

from deter import topdl

subs = [ ]
elems = [ ]
for i in range(0,2):
	print i
	subs.append(topdl.Substrate(name='link'+str(i)))

print subs
	
clients = ('client1','client2')
for index,elem_name in enumerate(clients):
    inf = topdl.Interface(name='inf000', substrate=['link'+str(index)])
    elem = topdl.Computer(name=elem_name, interface=[inf])
    elem.set_attribute('containers:node_type','openvz');
    elem.set_attribute('containers:partition',index);
    elem.set_attribute('startup','sudo python /share/magi/current/magi_bootstrap.py');
    elems.append(elem)
servers = ('server1')
for index in xrange(len(clients)):
	inf = topdl.Interface(name='inf'+str(format(index,'03d)),substrate=['link'+str(index)])
	elem = topdl.Computer(name='server1', interface = [inf])
	
elem.set_attribute('container:node_type','embedded_pnode');
elem.set_attribute('container:partition', len(clients)+1);
elem.set_attribute('startup','sudo python /share/magi/current/magi_bootstrap.py');
elems.append(elem)
"""elem = topdl.Computer(name='control')
elem.set_attribute('containers:node_type','embedded_pnode');
elem.set_attribute('containers:partition','1');
elem.set_attribute('startup','sudo python /share/magi/current/magi_bootstrap.py');
elems.append(elem)"""

top = topdl.Topology(substrates=subs, elements=elems)
f = open ('exp2.xml','w+')
f.write( topdl.topology_to_xml(top, top ='experiment'))
f.close()
#print topdl.topology_to_xml(top, top='experiment')
