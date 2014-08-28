#!/usr/bin/env python

from magi.messaging import api
from magi.orchestrator import AAL, Orchestrator
from magi.orchestrator.parse import AALParseError
from magi.util import helpers
from socket import gaierror # this should really be wrapped in daemon lib.
from sys import exit

import logging.handlers
import optparse
import os.path
import signal

def sigusr1_handler(signum, name):
    '''Set the flag in the Orchestrator module that causes current
    state to be printed to stdout when we get signal USR1.'''
    Orchestrator.show_state = True

if __name__ == '__main__':
    optparser = optparse.OptionParser()
    optparser.add_option("-p", "--project", dest="project", help="Project name")
    optparser.add_option("-e", "--experiment", dest="experiment", help="Experiment name")
    optparser.add_option("-b", "--bridge",
                         dest="bridge",
                         help="Address of the bridge node to join the "
                              "messaging overlay (ex: control.exp.proj)")
    optparser.add_option("-r", "--port",
                         dest="port", type="int", default=18808,
                         help="The port to connect to on the bridge node.")
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
                              "Deter Ops (users.deterlab.net).",
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

    logLevels = {
        'none': 100,
        'all': 0,
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    log_format = '%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-8s %(message)s'
    log_datefmt = '%m-%d %H:%M:%S'
    
    if options.logfile:
        # Roll over the old log and create a new one
        # Note here that we will have at most 5 logs
        if os.path.isfile(options.logfile):
            needroll = True
        else:
            needroll = False
        handler = logging.handlers.RotatingFileHandler(options.logfile, backupCount=5)
        if needroll:
            handler.doRollover() 
        handler.setFormatter(logging.Formatter(log_format, log_datefmt))
        root = logging.getLogger()
        root.setLevel(logLevels[options.loglevel])
        root.handlers = []
        root.addHandler(handler)
    else:
        logging.basicConfig(format=log_format,
                            datefmt=log_datefmt,
                            level=logLevels[options.loglevel])

    logging.info('set log level to %s (%d)' % (options.loglevel,
                                               logLevels[options.loglevel]))

    for f in options.events:
        if not os.path.exists(f):
            logging.critical('Events file %s does not exist. Exiting.' % f)
            exit(1)

    try:
        aal = AAL(options.events, dagdisplay=options.display, groupBuildTimeout=options.groupBuildTimeout)
        #aal = AAL(options.events, groupBuildTimeout=options.groupBuildTimeout)
    except AALParseError as e:
        logging.critical('Unable to parse events file: %s', str(e))
        exit(2)

    # Give the ability to just display the events file 
    if options.justparse:
        exit(0)
    
    if not options.bridge:
        if not options.project or not options.experiment:
            optparser.print_help()
            optparser.error("Missing project and/or experiment name")
        (bridgeNode, bridgePort) = helpers.getBridge(project=options.project, experiment=options.experiment)
    else:
        bridgeNode = options.bridge
        bridgePort = options.port
        
    try:   
        tunnel_cmd = None
        if options.tunnel:
            tunnel_cmd = helpers.createSSHTunnel('users.deterlab.net', bridgePort, bridgeNode, bridgePort, options.username)
            bridgeNode = '127.0.0.1'
            logging.info('Tunnel setup done')
                
        try:
            messaging = api.ClientConnection(options.name, bridgeNode, bridgePort)
        except gaierror as e:
            logging.critical('Error connecting to %s: %s', options.control, str(e))
            exit(3)
    
        signal.signal(signal.SIGUSR1, sigusr1_handler)
    
        orch = Orchestrator(messaging, aal, dagdisplay=options.display, verbose=options.verbose,
                            exitOnFailure=options.exitOnFailure,
                            useColor=(not options.nocolor))
        orch.run()
        
    finally:
        if tunnel_cmd:
            helpers.terminateProcess(tunnel_cmd)
            
    # GTL - we need to determine the outcome and return the
    # appropriate value here.
    exit(0)
