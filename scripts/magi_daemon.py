#!/usr/bin/env python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import logging.handlers
import os
import optparse
import sys

from magi.daemon.daemon import Daemon
from magi.util import config, helpers
from magi import __version__ 

handler = None

if __name__ ==  '__main__':
#    signal.signal(signal.SIGINT, signal.SIG_DFL)

    optparser = optparse.OptionParser(description="Script to start MAGI")
    optparser.add_option("-f", "--logfile", dest="logfile", action='store', help="Log to specified file, Default: %default, ex: -f file.log")
    optparser.add_option("-l", "--loglevel", dest="loglevel", default="INFO", help="set logger to level ALL, DEBUG, INFO, WARNING, ERROR. Default: %default, ex: -l DEBUG")
    optparser.add_option("-t" , "--timeformat", dest="timeformat", action='store', default="%m-%d %H:%M:%S", help="Set the format of the time epoch, Default: %default")     
    optparser.add_option("-c", "--nodeconf", dest="nodeconf", default=config.NODECONF_FILE, 
                         help="Specify location of the node configuration file, Default: %default, ex: -c localnode.conf ")

    (options, args) = optparser.parse_args()
    
    nodeConfig = config.loadNodeConfig(options.nodeconf)
    
    if not options.logfile:
        options.logfile = os.path.join(config.getLogDir(), "daemon.log")
    
    helpers.makeDir(os.path.dirname(options.logfile))
            
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

    # 08/082013: Note that to get msec time resolution we need to add change the formatter like so 
    # '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(threadName)s %(message)s', options.timeformat)
    handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(threadName)s %(message)s', options.timeformat))
    log = logging.getLogger()
    log.setLevel(helpers.logLevels.get(options.loglevel.upper(), logging.INFO))
    log.handlers = []
    log.addHandler(handler)
    
    try:      
        from magi.util import database 
        if database.isDBEnabled:
            log.info("Setting up database log handler")
            from magi.util.databaseLogHandler import MongoHandler
            dbhost = database.getCollector()
            #Making sure that the database server is up and running
            log.addHandler(MongoHandler.to(database.DB_NAME, database.COLLECTION_NAME, host=dbhost, port=database.DATABASE_SERVER_PORT))
    
        pid = os.getpid()
        try:
            fpid = open(config.MAGIPID_FILE, 'w')
            fpid.write(str(pid))
            fpid.close()
        except:
            pass
    
        transports = nodeConfig.get('transports', [])
        testbedInfo = nodeConfig.get('localInfo', {})
        localname = testbedInfo.get('nodename')
                
        # Some system initialization
        logging.info("MAGI Version: %s", __version__)
        logging.info("Started magi daemon on %s with pid %s", localname, pid)
    #    if not options.nodataman: logging.info("DB host: %s", dbhost)
        daemon = Daemon(localname, transports)
        daemon.run() 
        # Application will exit once last non-daemon thread finishes

    except Exception, e:
        log.exception("Exception while starting daemon process")
        sys.exit(e)