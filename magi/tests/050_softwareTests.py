#!/usr/bin/env python

import unittest2
import logging
import subprocess
import platform
from magi.util.software import *
from magi.tests.util import softwareRequired

class SoftwareInstallerTest(unittest2.TestCase):
	"""
		Testing of daemon software check and install
	"""

	@classmethod
	def setUpClass(cls):
		softwareRequired("SoftwareInstaller")
		cls.archivedir = os.path.join(os.path.dirname(__file__), 'archivetest')
		subprocess.call("chmod 775 %s" % (cls.archivedir + '/*.install'), shell=True)

	def isInstalled(self, program):
		""" careful on exception caught as that is how unittest indicates things """
		try:
			subprocess.call(program, stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
			return True
		except:
			return False

	def test_Yum(self):
		""" Test install using yum """
		installer = YumInstaller()
		if self.isInstalled("yum"):
			self.assertFalse(installer.install('httpdX'))
			self.assertTrue(installer.install('httpd'))
			self.assertTrue(installer.install('httpd'))
		else:
			self.assertFalse(installer.install('httpdX'))
			self.assertFalse(installer.install('httpd'))
			self.assertFalse(installer.install('httpd'))

	def test_Apt(self):
		""" Test install using apt-get """
		installer = AptInstaller()
		if self.isInstalled("apt-get"):
			self.assertFalse(installer.install('httpdX'))
			self.assertTrue(installer.install('apache2'))
			self.assertTrue(installer.install('apache2'))
		else:
			self.assertFalse(installer.install('httpdX'))
			self.assertFalse(installer.install('apache2'))
	 		self.assertFalse(installer.install('apache2'))

	def test_Archive(self):
		""" Test install from directory of archives files """
		installer = ArchiveInstaller(dir=os.path.join(os.path.dirname(__file__), 'archivetest'))
		self.assertTrue(installer.install('tarfile'))
		self.assertTrue(installer.install('zipfile'))
		self.assertFalse(installer.install('missing'))

	def test_Source(self):
		""" Test install from a source build """
		installer = SourceInstaller(dir=os.path.join(os.path.dirname(__file__), 'archivetest'))
		self.assertTrue(installer.install('testsrc'))
		self.assertFalse(installer.install('badsrc'))
		self.assertFalse(installer.install('missing'))

	def test_require(self):
		""" Test use of require function """
		if not os.path.exists('/etc/magi.conf'):
			raise unittest2.SkipTest("Skipping require test as /etc/magi.conf not present")
		requireSoftware('apache2','httpd', update=True, force=True)
		requireSoftware('jonpy')
		requireSoftware('apache2','httpd')

	def test_osSpecific(self):
		"""Tests OS specific installations."""
		if not os.path.exists('/etc/magi.conf'):
			raise unittest2.SkipTest("Skipping require test as /etc/magi.conf not present")

		if platform.system().lower() == 'linux':
			requireSoftware('ipfw', os='freebsd')
			# not a great test as installation would've failed anyway on linux. 
			# but requireSoftware does not return a fail/success value.
			self.assertFalse(self.isInstalled('ipfw'))



if __name__ == '__main__':
	import signal
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	hdlr = logging.StreamHandler()
	hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
	root = logging.getLogger()
	root.handlers = []
	root.addHandler(hdlr)
	root.setLevel(logging.DEBUG)
	unittest2.main(verbosity=2)

