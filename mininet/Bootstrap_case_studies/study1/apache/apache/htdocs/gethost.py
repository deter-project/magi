#!/usr/bin/env python

import jon.cgi as cgi
import jon.fcgi as fcgi
import subprocess
import re
from cStringIO import StringIO


class Handler(cgi.Handler):
	def process(self, req):
		address = req.environ['REMOTE_ADDR']
		cmd = ['grep', '-w', address, '/etc/hosts']
                (output, err) = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

		routes = StringIO(output)
		name = output 

		for line in routes:
    			tokens = re.split(r'[ \t\n]', line);
			if len(tokens) >= 4:
				name = tokens[3]
			else:
				name = line 
		databuf = "<html><body><p>\nSource is node named %s with IP %s\n</p></body></html>\n" % (name, address)
		length = len(databuf)
		req.set_buffering(req.params.get('buffer', False))
		req.set_header("Content-Type", "text/html")
		req.set_header("Content-Length", str(length))

		req.write(databuf)

fcgi.Server({fcgi.FCGI_RESPONDER: Handler}).run() 

