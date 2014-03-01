#!/usr/bin/python
# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

import sys
import signal
import logging.handlers
import os
from optparse import (OptionParser,BadOptionError,AmbiguousOptionError)

# This logging stuff needs to happen before module call getLogger
#from magi.util.log import AgentLogger, AgentRootLogger
#logging.setLoggerClass(AgentLogger)
#logging.root = AgentRootLogger(logging.WARNING)
#logging.Logger.root = logging.root
#logging.Logger.manager.root = logging.root
#logging.basicConfig() # for everything before real logging is setup

from magi.daemon.daemon import Daemon
from magi.util import config
from magi import __version__ 

handler = None

class PassThroughOptionParser(OptionParser):
    """
        An unknown option pass-through implementation of OptionParser.
        When unknown arguments are encountered, bundle with largs and try again,
        until rargs is depleted.  
        sys.exit(status) will still be called if a known argument is passed
        (e.g. missing arguments or bad argument types, etc.)

        override the epilog formatter that strips tabs and newlines using testwrap.fill 

    """

    def _process_args(self, largs, rargs, values):
        while rargs:
            try:
                OptionParser._process_args(self,largs,rargs,values)
            except (BadOptionError,AmbiguousOptionError), e:
                largs.append(e.opt_str)

    def format_epilog(self,formatter):
        return self.epilog


if __name__ ==  '__main__':
#    signal.signal(signal.SIGINT, signal.SIG_DFL)

    epilog = """  -l LOGGER LEVEL \tSet the logging level for the module LOGGER to level
                \tLEVEL where level can be
                \t1=ALL, 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR. 
                \tDefault: magi INFO, ex: -l magi.messaging INFO\n"""
    optparse = PassThroughOptionParser(description="Start the magi daemon", epilog=epilog)
    optparse.add_option("-f", "--logfile", dest="logfile", action='store', default=config.MAGILOG+'/daemon.log', help="Log to specified file, Default: %default, ex: -f file.log")
    optparse.add_option("-t" , "--timeformat", dest="timeformat", action='store', default="%m-%d %H:%M:%S", help="Set the format of the time epoch, Default: %default")     
    # 9/6/2013: 
    # optparse does not allow the append actions for string type! Hence it is implemented as position arguments 
    # Also needed to extend the OptionParser to ignore the options is does not understand at the command line 
    #optparse.add_option("-l", "--loggerlevel", dest="loggerlevel", nargs=2, action='append', default="magi 20", help="set logger to level 1=ALL, 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR. Default: %default, ex: -l magi 1")
    optparse.add_option("-c", "--magiconf", dest="magiconf", default=config.MAGILOG+'/magi.conf', help="Specify location of the magi configuration file, Default: %default, ex: -c localconfig.conf ")
    optparse.add_option("-D", "--nodataman", dest="nodataman", action="store_true", default=False, help="Data manager not setup up.") 

    (options, args) = optparse.parse_args()
#    print options, 
#    print args 

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
    root.setLevel(logging.INFO)
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

    # Parse the positional options 
    ii=0
    while ii < len(args):
        arg = args[ii]
        ii = ii + 1 
        logging.debug("args: %s", args) 
        if arg == '-l':
            (logger,level) = args[ii:ii+2]
            logging.getLogger(logger).setLevel(int(level))
            ii = ii + 2
        else:
            logging.debug("Bad option %s", arg)
            optparse.print_help()


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

