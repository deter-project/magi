#!/usr/bin/python

from mininet.net import Mininet
from mininet.topo import SingleSwitchTopo

from bottle import route, run, template

net = Mininet( topo=SingleSwitchTopo( 2 ) )

@route('/cmd/<node>/<cmd>')
def cmd( node='h1', cmd='hostname' ):
    out, err, code = net.get( node ).pexec( cmd )
    return out + err

#def cmd( node='h2', cmd='hostname' ):
#   out, err, code = net.get( node ).pexec( cmd )
#   return out + err

@route('/stop')
def stop():
    net.stop()

net.start()
h1 = net.getNodeByName('h1')
# add command to start iperf server
run(host='localhost', port=8080 )
