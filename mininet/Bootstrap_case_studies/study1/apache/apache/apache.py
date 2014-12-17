#!/usr/bin/env python

# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from magi.util.agent import SharedServer, agentmethod
from magi.util.execl import run, pipeIn
from magi.util.processAgent import initializeProcessAgent

import errno
import logging
import os
import glob
import stat
import time
import re
import sys


log = logging.getLogger(__name__)

class ApacheAgent(SharedServer):
	"""
		Provides interface for starting and stopping a shared instance of the Apache server
	"""

	def __init__(self):
		SharedServer.__init__(self)

		# List of variables that the user can set
		self.variableconf = {
			'StartServers': 5,
			'MaxClients': 200,
			'MaxRequestsPerChild': 120,
			'Timeout': 300,
			'KeepAlive': 'On',
			'MaxKeepAliveRequests': 100,
			'KeepAliveTimeout': 30
		}

		# List of loadable modules we need to load for things to work
		self.loadmodules = {
			'fastcgi_module': 'mod_fastcgi.so',
			'mime_module': 'mod_mime.so',
			'gnutls_module': 'mod_gnutls.so'
		}

		self.configfile = None
		self.moduledir = None
		self.configpatt = re.compile(r'SERVER_CONFIG_FILE="(.*)"')

		self.terminateserver()

		self.setConfig(None)

		# GTL - there is a race condition somewhere. Sleep a few seconds, 
		# so we loose the race.
		time.sleep(2)


	def locateConfig(self):
		if self.configfile is not None:
			return self.configfile

		self.configfile = None

		apachePaths = ['/etc/apache2', '/usr/lib/apache2', '/usr/local/lib/apache2']

		for line in pipeIn("apache2ctl -V", close_fds=True):
			match = self.configpatt.search(line)
			if match is not None:
				# Apache on ubuntu 12.04 does not give the full path to the conf file, 
				# so we need to look for it in a few well known places. 
				filename = match.group(1)
				if not os.path.dirname(filename):
					for path in apachePaths:
						log.info("path is %s" % path)
						if os.path.exists(os.path.join(path, filename)):
							self.configfile = os.path.join(path, filename)
							#log.error("found %s" % os.path.join(path, filename) )
							break
				else:
					self.configfile = filename
					log.info("filename = %s" % filename)

		if self.configfile is None:
			raise OSError(errno.ENOENT, "Unable to locate apache config at this time, maybe later?")

		# Need to find out where this distro stuck the apache modules
		for path in apachePaths:
			if os.path.exists(os.path.join(path, 'modules', 'mod_fastcgi.so')):
				self.moduledir = os.path.join(path, 'modules')

		# Also need to find out where it stored the mime.types file
		for root, dirs, files in os.walk('/etc'):
			if 'mime.types' in files:
				self.mimetypes = os.path.join(root, 'mime.types')
			
		# This get added to the variable conf for each integration with config write
		self.variableconf['TypesConfig'] = self.mimetypes
		self.variableconf['ServerRoot'] = os.path.dirname(self.configfile)  # i.e. "/etc/apache"
		self.variableconf['DocumentRoot'] = os.path.join(os.path.dirname(__file__), 'htdocs') # i.e. next to apache.py

		# GTL HACK - setup.py installs all py files are read/write no execute. BUt we have scripts in 
		# htdocs which are scripts and must be executable. So we change them by hand here, in a not
		# standard place. GTL TODO force sdist (setup.py) to keep file permissions in htdocs dir. 
		for f in glob.glob(os.path.join(self.variableconf['DocumentRoot'], '*.py')):
			# ugo --> r-x
			os.chmod(f, stat.S_IEXEC | stat.S_IREAD | stat.S_IXGRP | stat.S_IRGRP | stat.S_IXOTH | stat.S_IROTH)

		return self.configfile


	def runserver(self):
		""" subclass implementation """
		run("apache2ctl start", close_fds=True)
		log.info('Apache started.')
		return True


	def terminateserver(self):
		""" subclass implementation """
		run("apache2ctl stop", close_fds=True)
		log.info('Apache stopped.')
		return True


	@agentmethod(kwargs="any key/value arguments for apache conf file")
	def setConfig(self, msg, **kwargs):
		"""
			Set configuration values in the apache.conf file, doesn't
			take effect until the next time apache is actually stopped and started
		"""
		for k, v in kwargs.iteritems():
			if k not in self.variableconf:
				log.error("Trying to write config variable %s, doesn't exist" % k)
				continue
			self.variableconf[k] = str(v)
		
		# Modify the conf file
		try:
			confFile = self.locateConfig()
			log.info("Writing our apache configuration to %s" % confFile)
			with open(confFile, 'w') as conf:
				# load module first
				for k, v in self.loadmodules.iteritems():
					conf.write("LoadModule %s %s/%s\n" % (k, self.moduledir, v))
				# basic config next
				for k, v in self.variableconf.iteritems():
					conf.write("%s %s\n" % (k,v))
				# then out static conf
				conf.write(self.staticconf)
		except Exception, e:
			log.error("Failed to update config file: %s", e, exc_info=1)

			
	staticconf = """
UseCanonicalName Off
ServerTokens Prod
ServerSignature Off
HostnameLookups Off
Listen 80
User daemon
Group daemon
ServerAdmin you@example.com
ErrorLog /var/log/apacheerror.log
LogLevel info 
DefaultType text/plain

FastCgiConfig -initial-env PATH

Options Indexes FollowSymLinks ExecCGI
AddHandler fastcgi-script .py

# minimal https config
Listen 443
GnuTLSCache dbm /var/cache/apache2/mod_gnutls_cache
<VirtualHost *:443>
  GnuTLSEnable on
  GnuTLSCertificateFile /etc/ssl/certs/ssl-cert-snakeoil.pem
  GnuTLSKeyFile /etc/ssl/private/ssl-cert-snakeoil.key
  GnuTLSPriorities NORMAL
</VirtualHost>

"""
def getAgent():
	return ApacheAgent()

if __name__ == "__main__":
	agent = ApacheAgent()
	kwargs = initializeProcessAgent(agent, sys.argv)
	agent.run()
