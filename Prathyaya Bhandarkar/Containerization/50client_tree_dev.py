#!/usr/bin/env python

from deter import topdl

subs = [ ]
elems = [ ]
intf = [ ]
clients = [ ]
number_of_clients = 50
for i in range(0,number_of_clients):
	print i
	subs.append(topdl.Substrate(name='link'+str(i)))

container_num = 0
for i in range(number_of_clients):
	client = 'client'+str(i)
	clients.append(client)
	
#clients = ('client1','client2')
for index,elem_name in enumerate(clients):
	if index <24:
		inf_list = []
		inf = topdl.Interface(name='inf000', substrate=['link'+str(index)])
		inf_list.append(inf)
		inf = topdl.Interface(name='inf001', substrate=['link'+str(index+24)])
		inf_list.append(inf)
		
	else:
		inf_list = []
		inf = topdl.Interface(name='inf000', substrate=['link'+str(index)])
		inf_list.append(inf)
		
	elem = topdl.Computer(name=elem_name, interface=inf_list)		
	elem.set_attribute('containers:node_type','openvz');
	elem.set_attribute('containers:partition',container_num);
	elem.set_attribute('startup','sudo python /share/magi/dev/magi_bootstrap.py -p /share/magi/dev/');
	elems.append(elem)
	if ((index+1)%8) == 0:
		container_num = container_num+1

servers = ('server1')



for index in range(24):
	inf = topdl.Interface(name='inf'+str(format(index,'03d')),substrate=['link'+str(index)])
	intf.append(inf)
inf = topdl.Interface(name='inf'+str(format(48,'03d')),substrate=['link'+str(48)])
intf.append(inf)
inf = topdl.Interface(name='inf'+str(format(49,'03d')),substrate=['link'+str(49)])
intf.append(inf)
elem = topdl.Computer (name = 'server1', interface = intf)	
elem.set_attribute('containers:node_type','openvz');
elem.set_attribute('containers:partition', container_num+1);
elem.set_attribute('startup','sudo python /share/magi/dev/magi_bootstrap.py -p /share/magi/dev/');
elems.append(elem)
"""elem = topdl.Computer(name='control')
elem.set_attribute('containers:node_type','embedded_pnode');
elem.set_attribute('containers:partition','1');
elem.set_attribute('startup','sudo python /share/magi/current/magi_bootstrap.py');
elems.append(elem)"""

top = topdl.Topology(substrates=subs, elements=elems)
f = open ('50client_tree_dev.xml','w+')
f.write( topdl.topology_to_xml(top, top ='experiment'))
f.close()
#print topdl.topology_to_xml(top, top='experiment')
