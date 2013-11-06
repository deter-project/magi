#!/usr/bin/env python

import optparse
import sys

from magi.util.config import generateKeys

if __name__ == '__main__':
	optparser = optparse.OptionParser(description="Generates CA and node certificates for an experiment using SSL sockets") 
	optparser.add_option("-p", "--project", dest="project", help="The project name")
	optparser.add_option("-e", "--experiment", dest="experiment", help="The experiment name")
	optparser.add_option("-k", "--keydir", dest="keydir", help="Optional directory to store keys, default is /proj/P/exp/E/tbdata/")
	(options, args) = optparser.parse_args()

	if options.project is None or options.experiment is None:
		optparser.print_help()
		sys.exit(-1)

	if options.keydir is None:
		options.keydir = "/proj/%s/exp/%s/tbdata" % (options.project, options.experiment)

	generateKeys(project=options.project, experiment=options.experiment, keydir=options.keydir)


