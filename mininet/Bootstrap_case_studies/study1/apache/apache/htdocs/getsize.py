#!/usr/bin/env python

import jon.cgi as cgi
import jon.fcgi as fcgi
import logging

databuf = """
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
0123456789012345678901234567890123456789012345678
"""

logging.basicConfig(filename='/tmp/apache.log',level=logging.DEBUG)

class Handler(cgi.Handler):
	def process(self, req):
		length = int(req.params.get('length', 0))
		req.set_buffering(req.params.get('buffer', False))
		req.set_header("Content-Type", "text/plain")
		req.set_header("Content-Length", str(length))
		
		if length > 100000:
			n100k = length/100000
			rem = length%100000
			f = open('100k.bin','r')
			s100k = f.read()
			f.close()
			for ii in range(n100k): 
				req.write(s100k)
			req.write(s100k[0:rem])
		
		else:
			full = length/500
			rem = length%500
			for ii in range(full):
				req.write(databuf[0:500])
			req.write(databuf[0:rem])

fcgi.Server({fcgi.FCGI_RESPONDER: Handler}).run() 

