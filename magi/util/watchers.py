# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import os
import stat
import time
import logging

log = logging.getLogger(__name__)

class DirWatcher(object):
	""" Support class for checking for new/modified/deleted files in a directory """

	def __init__(self, watcheddir):
		""" Create a new Watcher object """
		if not os.path.isdir(watcheddir):
			os.makedirs(watcheddir)

		self.watcheddir = watcheddir
		self.filelist = []
		self.lastread = 0

	def getChanges(self):
		""" check self.watcheddir for changes and return a tuple of lists """
		deleted = []
		modified = []

		diskfilelist = os.listdir(self.watcheddir)
		# Look for deleted files
		for f in self.filelist:
			if f not in diskfilelist:
				deleted.append(f)

		self.filelist = diskfilelist
		# Look for new and modified files
		for f in self.filelist:
			fullname = self.watcheddir+"/"+f
			modt = os.stat(fullname)[stat.ST_MTIME]
			# Use >= +1 so that we catch mods that happen in the same second as last read
			if (modt+1 >= self.lastread): 
				modified.append(f)

		self.lastread = time.time()
		return (deleted, modified)



class FileWatcher(object):
	""" Suport class for tailing a data file """

	def __init__(self, filename):
		self.filename = filename
		self.marker = 0

	def getNextLine(self):
		fp = None
		try:
			try:
				size = os.stat(self.filename)[stat.ST_SIZE]
				if (size > self.marker):
					fp = open(self.filename)
					fp.seek(self.marker)
					line = fp.readline()
					self.marker += len(line)
					line = line.strip()
					if (line != ""):
						return line
			except Exception, e:
				if e.errno != 2: # No Such File
					log.info("Log Read Exception", exc_info=1)
				else:
					log.debug("Can't find file %s, setting marker to 0" % (self.filename))
					self.marker = 0
		finally:
			if fp is not None:
				fp.close()

		return None


