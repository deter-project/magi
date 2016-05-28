#!/usr/bin/env python

from magi.messaging import api
from magi.orchestrator import AAL, Orchestrator
from magi.orchestrator.parse import AALParseError
from magi.db import ROUTER_SERVER_PORT

from magi.util import helpers, config
from socket import gaierror # this should really be wrapped in daemon lib.
from sys import exit

import logging.handlers
import optparse
import os.path
import signal
import time

lastSignalRcvd = 0
orch = None

def signal_handler(signum, frame):
    '''Set the flag in the Orchestrator module that causes current
    state to be printed to stdout when we get signal SIGINT.'''
    global lastSignalRcvd
    global orch
    if time.time() - lastSignalRcvd < 2:
        if orch:
            print 'Cleaning up and exiting'
            orch.stop(doTearDown=True)
        else:
            print 'Exiting'
            exit(0)
    else:
        Orchestrator.show_state = True
        print "Send SIGINT signal (ctrl+c) again within next 2 seconds to quit"
    lastSignalRcvd = time.time()
    
if __name__ == '__main__':
    optparser = optparse.OptionParser()
    optparser.add_option("-b", "--bridge",
                         dest="bridge",
                         help="Address of the bridge node to join the "
                              "messaging overlay (ex: control.exp.proj)")
    
    optparser.add_option("-c", "--control",
                         dest="bridge",
                         help="Address of the bridge node to join the "
                              "messaging overlay (ex: control.exp.proj). "
                              "This option exists for backward compatibility.")
    
    optparser.add_option("-r", "--port",
                         dest="port", type="int", default=18808,
                         help="The port to connect to on the bridge node.")
    
    optparser.add_option("--dbhost",
                         dest="dbhost",
                         help="Address of the host running the database")
    
    optparser.add_option("--dbport",
                         dest="dbport", type="int", default=ROUTER_SERVER_PORT,
                         help="The port to connect to the database.")
    
    optparser.add_option("-i", "--config", dest="config", help="Experiment configuration file location")
    
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")

    optparser.add_option("-f", "--events",
                         dest="events",
                         help="The procedure.aal file(s) to use. Can be specified"
                              " multiple times for multiple AAL files",
                         action="append", 
                         default=[])  # real default added below
    
    optparser.add_option("-o", "--logfile",
                         dest="logfile",
                         help="If given, log to the file instead of the "
                              "console (stdout).")
    
    optparser.add_option("-l", "--loglevel",
                         dest="loglevel",
                         help="The level at which to log. Must be one of "
                              "none, debug, info, warning, error, or "
                              "critical. Default is info.",
                         default='info',
                         choices=['none', 'all', 'debug', 'info', 'warning',
                                  'error', 'critical'])
    
    optparser.add_option("-n", "--name", 
                         dest="name",
                         help="Name using which to connect to the messaging plane. " 
                              "Default: %default",
                         default="pyorch")
    
    optparser.add_option("-x", "--exitOnFailure",
                         dest="exitOnFailure",
                         help="If any method call fails (returns False), then"
                              " exit all streams, unload all agents, and exit"
                              " the orchestrator. Default value is True",
                         default=True)
    
    optparser.add_option("-g", "--groupBuildTimeout",
                         dest="groupBuildTimeout",
                         type="int", default=20000,
                         help="When building the initial groups for agents "
                              "in the given AAL, use the timeout given (in "
                              "milliseconds) when waiting for group "
                              "formation to complete.")
    
    optparser.add_option("--nocolor",
                         dest="nocolor",
                         help="If given, do not use color in output.",
                         action='store_true')
    
    optparser.add_option("-v", "--verbose",
                         dest="verbose",
                         help="Tell orchestrator to print info about what "
                              "its doing",
                         default=False,
                         action="store_true")
    
    optparser.add_option("-t", "--tunnel",
                         dest="tunnel",
                         help="Tell orchestrator to tunnel data through "
                              "Deter Ops (users.deterlab.net). Must specify "
                              "the bridge node.",
                         default=False,
                         action="store_true")
    
    optparser.add_option("-u", "--username", 
                         dest="username", 
                         help="Username for creating tunnel. Required only if "
                              "different from current shell username.")
    
    optparser.add_option("-d", "--display", 
                         dest="display",
                         help="Display the procedure execution graphically",
                         default=False,
                         action="store_true")
    
    optparser.add_option("-j", "--justparse", 
                         dest="justparse",
                         help="Parse and display the procedure file specified with -f",
                         default=False,
                         action="store_true")

    (options, args) = optparser.parse_args()

    if not options.events:
        optparser.print_help()
        optparser.error("Missing events file")

    log = logging.getLogger()
    log.handlers = []
    
    if options.logfile:
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
    
    else:
        handler = logging.StreamHandler()
        
    handler.setFormatter(logging.Formatter(helpers.LOG_FORMAT_MSECS, helpers.LOG_DATEFMT))
    log.setLevel(helpers.logLevels.get(options.loglevel.upper(), logging.INFO))
    log.addHandler(handler)

    log = logging.getLogger(__name__)
    log.info('set log level to %s (%d)' % (options.loglevel, log.getEffectiveLevel()))

    for f in options.events:
        if not os.path.exists(f):
            logging.critical('Events file %s does not exist. Exiting.' % f)
            exit(1)

    try:
        aal = AAL(options.events, dagdisplay=options.display, groupBuildTimeout=options.groupBuildTimeout)
    except AALParseError as e:
        logging.critical('Unable to parse events file: %s', str(e))
        exit(2)

    # Give the ability to just display the events file 
    if options.justparse:
        exit(0)
    
    bridgeNode = None
    bridgePort = options.port
    dbHost = None
    dbPort = options.dbport
    
    if options.bridge:
        bridgeNode = options.bridge
        dbHost = options.bridge
        
    if options.dbhost:
        dbHost = options.dbhost
    
    if not bridgeNode or not dbHost:  
        if not options.config:
            if not (options.project and options.experiment):
                optparser.print_help()
                optparser.error("Missing bridge information and "
                                "experiment configuration information")
                
            options.config = helpers.getExperimentConfigFile(options.project, 
                                                             options.experiment)
        
        while not os.path.isfile(options.config):
            log.info("Config file might still be in the process of being created.")
            time.sleep(5)
        
        # Set the context by loading the experiment configuration file
        config.loadExperimentConfig(options.config)
        
        (bridgeNode, bridgePort) = helpers.getBridge(experimentConfigFile=options.config)
        
        (dbHost, dbPort) = helpers.getExperimentDBHost(experimentConfigFile=options.config)
                    
    try:   
        tunnel_cmd = None
        db_tunnel_cmd = None
        if options.tunnel:
            localDbPort = 27020
            tunnel_cmd = helpers.createSSHTunnel('users.deterlab.net', bridgePort, bridgeNode, bridgePort, options.username)
            db_tunnel_cmd = helpers.createSSHTunnel('users.deterlab.net', localDbPort, dbHost, dbPort, options.username)
            bridgeNode = '127.0.0.1'
            dbHost = '127.0.0.1'
            dbPort = localDbPort
            logging.info('Tunnel setup done')
                
        from magi_status import getStatus
        nodeSet = helpers.getNodesFromAAL(options.events)
        log.info("Making sure Magi daemons are up and listening")
        while True:
            try:
                (status, result) = getStatus(bridgeNode=bridgeNode, 
                                             bridgePort=bridgePort, 
                                             nodeSet=nodeSet,
                                             timeout=10)
                if status:
                    break
                log.info("Magi daemon on one or more nodes not up")
                log.info("Did not receive reply from %s", sorted(list(nodeSet-set(result.keys()))))
                nodeSet = nodeSet-set(result.keys())
            except:
                log.info("Magi daemon on one or more nodes not up")
                time.sleep(5)
        
        log.info("All magi daemons are up and listening")
        
        try:
            messaging = api.ClientConnection(options.name, bridgeNode, bridgePort)
        except gaierror as e:
            logging.error("Error connecting to %s: %d" %(bridgeNode, bridgePort))
            exit(3)
    
        signal.signal(signal.SIGINT, signal_handler)
        
        orch = Orchestrator(messaging, aal, dagdisplay=options.display, verbose=options.verbose,
                            exitOnFailure=options.exitOnFailure,
                            useColor=(not options.nocolor), dbHost=dbHost, dbPort=dbPort)
        orch.run()
        
    finally:
        if tunnel_cmd:
            helpers.terminateProcess(tunnel_cmd)
        if db_tunnel_cmd:
            helpers.terminateProcess(db_tunnel_cmd)
            
    # GTL - we need to determine the outcome and return the
    # appropriate value here.
    exit(0)
