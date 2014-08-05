#!/usr/bin/env python

from optparse import OptionParser
from magi.util import config
from magi.util import helpers
import yaml
import sys
import os

parser = OptionParser(description="Script to write a default /etc/node.conf")
parser.add_option("-f", "--force", dest="force", help="overwrite a current node config", default=False, action="store_true")
parser.add_option("-e", "--expconf", dest="expconf", action="store", default="/share/magi/current/experiment.conf", 
                  help="The experiment wide configuration file. The default is located at /share/magi/current/experiment.conf")
parser.add_option("-c", "--nodeconf", dest="nodeconf", action="store", default="/etc/node.conf", help="Location of the per node magi configuration file")
(options, args) = parser.parse_args()

# Check if force is required
if os.path.exists( options.nodeconf ):
    print "File exists", options.nodeconf  
    if not options.force:
        print "/etc/node.conf exists, will not overwrite without force option"
        sys.exit(-1)

fp = open(options.nodeconf, "w")
fp.write(yaml.safe_dump(config.createNodeConfig(experimentConfigFile=options.expconf)))
fp.close()

# Setup args by reading the MeSDL 
if options.expconf: 
    mesdl = helpers.loadYaml(options.expconf)
    print mesdl 

