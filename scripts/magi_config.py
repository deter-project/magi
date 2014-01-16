#!/usr/bin/env python

from optparse import OptionParser
from magi.testbed import testbed
from magi.util import config
import yaml
import sys
import os

parser = OptionParser(description="Script to write a default /etc/magi.conf")
parser.add_option("-f", "--force", dest="force", help="ovewrite a current magi config", default=False, action="store_true")
parser.add_option("-m", "--mesdl", dest="mesdl", action="store", default="/share/magi/current/mesdl.conf", help="The messaging overlay configuration file. The default is located at /share/magi/current/magi.conf")
parser.add_option("-c", "--magiconf", dest="magiconf", action="store", default="/etc/magi.conf", help="Location of the per node magi configuration file")
(options, args) = parser.parse_args()

# Check if force is required
if config.verifyConfig( options.magiconf ):
    print "File exists", options.magiconf  
    if not options.force:
        print "/etc/magi.conf exists, will not overwrite without force option"
        sys.exit(-1)

fp = open(options.magiconf, "w")
fp.write(yaml.safe_dump(config.createConfig(mesdl=options.mesdl)))
fp.close()

# Setup args by reading the MeSDL 
if options.mesdl: 
    mesdl = config.loadYaml(options.mesdl)
    print mesdl 

