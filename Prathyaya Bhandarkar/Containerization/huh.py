#!/usr/local/bin/python


from deter import topdl

top = topdl.Topology(substrates=[
    topdl.Substrate("link0", topdl.Capacity(100000.0, 'max'))],
    elements=[
	topdl.Computer('e1', interface=[topdl.Interface('link0')]),
	topdl.Computer('e2', interface=[topdl.Interface('link0')])])

f = open ('huhtest.xml','w+')
f.write( topdl.topology_to_xml(top, top ='experiment'))
f.close()
