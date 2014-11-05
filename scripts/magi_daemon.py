#!/usr/bin/env python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import logging.handlers
import os
import optparse
import sys

from magi.util import config, helpers
from magi import __version__ 

handler = None

if __name__ ==  '__main__':
#    signal.signal(signal.SIGINT, signal.SIG_DFL)

    optparser = optparse.OptionParser(description="Script to start MAGI")
    optparser.add_option("-f", "--logfile", dest="logfile", action='store', help="Log to specified file, Default: %default, ex: -f file.log")
    optparser.add_option("-l", "--loglevel", dest="loglevel", default="INFO", help="set logger to level ALL, DEBUG, INFO, WARNING, ERROR. Default: %default, ex: -l DEBUG")
    optparser.add_option("-c", "--nodeconf", dest="nodeconf", help="Specify location of the node configuration file, Default: %default, ex: -c localnode.conf ")

    (options, args) = optparser.parse_args()
    
    nodeConfig = config.loadNodeConfig(options.nodeconf)
    
    #import once the system is cofigured
    from magi.daemon.daemon import Daemon

    if not options.logfile:
        options.logfile = os.path.join(config.getLogDir(), "daemon.log")
    
    helpers.makeDir(os.path.dirname(options.logfile))
    helpers.makeDir(config.getTempDir())
            
    # Roll over the old log and create a new one
    # Note here that we will have at most 5 logs 
    # Need to check existence of file before creating the handler instance
    # This is because handler creation creates the file if not existent 
    if os.path.isfile(options.logfile):
        needroll = True
    else:
        needroll = False
    handler = logging.handlers.RotatingFileHandler(options.logfile, backupCount=5)
    if needroll:
        handler.doRollover()

    handler.setFormatter(logging.Formatter(helpers.LOG_FORMAT_MSECS, helpers.LOG_DATEFMT))
    log = logging.getLogger()
    log.setLevel(helpers.logLevels.get(options.loglevel.upper(), logging.INFO))
    log.handlers = []
    log.addHandler(handler)
    
    try:      
        pid = os.getpid()
        try:
            fpid = open(config.getMagiPidFile(), 'w')
            fpid.write(str(pid))
            fpid.close()
        except:
            pass
        
        # Some system initialization
        transports = nodeConfig.get('transports', [])
        testbedInfo = nodeConfig.get('localInfo', {})
        localname = testbedInfo.get('nodename')
        
        log.info("MAGI Version: %s", __version__)
        log.info("Started magi daemon on %s with pid %s", localname, pid)
        daemon = Daemon(localname, transports)
        daemon.run() 
        # Application will exit once last non-daemon thread finishes

    except Exception, e:
        log.exception("Exception while starting daemon process")
        sys.exit(e)