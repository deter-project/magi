#!/usr/bin/env python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import logging.handlers
import os
import optparse

from magi.daemon.daemon import Daemon
from magi.util import config, helpers
from magi import __version__ 

handler = None

if __name__ ==  '__main__':
#    signal.signal(signal.SIGINT, signal.SIG_DFL)

    optparser = optparse.OptionParser(description="Script to start MAGI")
    optparser.add_option("-f", "--logfile", dest="logfile", action='store', default=config.MAGILOG+'/daemon.log', help="Log to specified file, Default: %default, ex: -f file.log")
    optparser.add_option("-t" , "--timeformat", dest="timeformat", action='store', default="%m-%d %H:%M:%S", help="Set the format of the time epoch, Default: %default")     
    optparser.add_option("-l", "--loggerlevel", dest="loggerlevel", default="INFO", help="set logger to level ALL, DEBUG, INFO, WARNING, ERROR. Default: %default, ex: -l DEBUG")
    optparser.add_option("-c", "--magiconf", dest="magiconf", default=config.MAGILOG+'/magi.conf', help="Specify location of the magi configuration file, Default: %default, ex: -c localconfig.conf ")
    optparser.add_option("-D", "--nodataman", dest="nodataman", action="store_true", default=False, help="Data manager not setup up.") 

    (options, args) = optparser.parse_args()
    
    config.DEFAULT_MAGICONF = options.magiconf
    
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
    root = logging.getLogger()
    root.setLevel(helpers.logLevels.get(options.loggerlevel.upper(), logging.INFO))
    root.handlers = []
    root.addHandler(handler)
           
    if not options.nodataman:
        from magi.util import database
        dbhost = database.getDBHost()
        from magi.mongolog.handlers import MongoHandler
        dbname = 'magi'
        collectionname = 'log'
        connection = database.getConnection()
        root.addHandler(MongoHandler.to(dbname, collectionname, host=dbhost, port=27017))

    pid = os.getpid()
    try:
        fpid =  open(config.DEFAULT_MAGIPID, 'w')
        fpid.write(str(pid))
        fpid.close()
    except:
        pass

    confdata = config.loadConfig(options.magiconf)
    transports_ctrl = confdata.get('transports', [])
    transports_exp = confdata.get('transports_exp', [])
    testbedInfo = confdata.get('localinfo', {})
    localname = testbedInfo.get('nodename')
            
    # Some system initialization
    logging.info("MAGI Version: %s", __version__)
    logging.info("Started magi daemon on %s with pid %s", localname, pid)
    if not options.nodataman: logging.info("DB host: %s", dbhost)
    daemon = Daemon(localname, transports_ctrl, transports_exp, not options.nodataman)
    daemon.run() 
    # Application will exit once last non-daemon thread finishes

