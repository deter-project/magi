#!/usr/bin/env python

from subprocess import Popen
import optparse
import errno
import logging.handlers
import signal
import sys
import os
import glob
from os import path

#logging.basicConfig(level=logging.INFO)
LOG_FILENAME = '/tmp/magi_bootstrap.log'
log = logging.getLogger()
log.setLevel(logging.INFO)

# Check if log exists and should therefore be rolled
needRoll = False
if path.isfile(LOG_FILENAME):
        needRoll = True
        
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s'))
log.addHandler(handler)

if needRoll: 
        handler.doRollover()

class BootstrapException(Exception): pass
class TarException(BootstrapException): pass
class PBuildException(BootstrapException): pass
class CBuildException(BootstrapException): pass
class DException(BootstrapException): pass

def call(*popenargs, **kwargs):
        log.info("Calling %s" % (popenargs))
        if "shell" not in kwargs:
                kwargs["shell"] = True

        process = Popen(*popenargs, **kwargs)
        process.wait()
        return process.returncode

def installPython(base, check, commands):
        global rpath 

        try:
                exec("import " + check)
                log.info("%s already installed, checked with 'import %s'", base, check)
                return
        except ImportError:
                pass
    
        paths = [rpath + '/tarfiles/' + base + '*', rpath + '/' + base + '*']

        fail = False
        for path in paths:
                log.info("Looking for disitrbution at %s...",path)
                if call("tar -C /tmp -xzf %s" % path):
                        log.error("Failed to untar %s", base)
                        fail = True
                else:
                        fail = False
                        break

        if fail:
                raise TarException("Failed to untar %s" % base)

        os.chdir(glob.glob(os.path.join("/tmp", base+'*'))[0])  # Need glob as os.chdir doesn't expand
        if call("python setup.py %s -f" % commands):
                log.error("Failed to install %s with commands %s", base, commands)
                raise PBuildException("Unabled to install with %s,%s" % (base, commands))

def installC(base, check):
        global rpath
        
        if os.path.exists(check):
                log.info("%s already installed, found file %s", base, check)
                return

        paths = [rpath+'/tarfiles/'+base+'*', rpath+'/'+base+'*']
        fail = False
        for path in paths:
                if call("tar -C /tmp -xzf %s" % path):
                        fail = True
                else:
                        fail = False
                        break

        if fail:
                raise TarException("Failed to untar %s" % base)

        os.chdir(glob.glob(os.path.join("/tmp", base+'*'))[0])  # Need glob as os.chdir doesn't expand
        if call("./configure") or call("make") or call("make install"):
                log.error("Failed to install %s", base)
                raise CBuildException("Unabled to install %s" % base)

def isInstalled(program):
        try:
                import subprocess
                subprocess.call(program, stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
                return True
        except:
                return False

def verifyPythonDevel():
        import distutils.sysconfig as c
        if not os.path.exists(os.path.join(c.get_python_inc(), 'Python.h')):
                if isInstalled('yum'):
                        call("yum install -y python-devel python-setuptools")
                elif isInstalled('apt-get'):
                        call("apt-get -qq install -y python-dev python-setuptools")
        if not os.path.exists(os.path.join(c.get_python_inc(), 'Python.h')):
                log.error("Python development not installed, nor can we use the local package manager to do so")
                
def is_os_64bit():
        import platform
        return platform.machine().endswith('64')

if __name__ == '__main__':
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        global rpath 
        
        optparser = optparse.OptionParser(description="Bootstrap script that can be used to install, configure, and start MAGI")
        optparser.add_option("-n", "--nokeys", dest="nokeys", action="store_true", default=False, help="Option to ignore creation and waiting for SSL certificates")
        optparser.add_option("-p", "--path", dest="rpath", default="/share/magi/current", help="Location of the distribution") 
        optparser.add_option("-q", "--quiet", dest="dargs", action="store_false", default=True, help="Silence debugging information; default behavior") 
        optparser.add_option("-U", "--noupdate", dest="noupdate", action="store_true", default=False, help="Do not update the system before installing Magi")
        optparser.add_option("-N", "--noinstall", dest="noinstall", action="store_true", default=False, help="Do not install magi and the supporting libraries") 
        optparser.add_option("-v", "--verbose", dest="dargs", action="store_true", default=False, help="Include debugging information") 
        optparser.add_option("-f", "--force", dest="force", action="store_true",default=False, help="Recreate magi.conf even if present. Cannot use along with -c (see below)")
        optparser.add_option("-m", "--mesdl", dest="mesdl", action="store", default=None, help="Path to the messaging overlay configuration file")  
        optparser.add_option("-c", "--magiconf", dest="magiconf", action="store", default=None, help="Path to the local node magi configuration file. Cannot use along with -f (see above)")
        optparser.add_option("-d", "--dbconf", dest="dbconf", action="store", default=None, help="Path to the data management configuration file")
        optparser.add_option("-D", "--nodataman", dest="nodataman", action="store_true", default=False, help="Do not install ans setup data manager") 
                
        (options, args) = optparser.parse_args()

        if options.magiconf and options.force == True:
                optparser.error("Options -c and -f are mutually exclusive. Please specify only one")

        rpath = options.rpath 

        if (sys.version_info[0] == 2) and (sys.version_info[1] < 5):
                sys.exit("Only works with python 2.5 or greater")

        if (not options.noupdate) and (not options.noinstall):  # double negative
                if isInstalled('yum'):
                        call("yum update")
                elif isInstalled('apt-get'):
                        call("apt-get -y update")
                else:
                        msg = 'I do not know how to update this system. Platform not supported. Run with --noupdate or on a supported platform (yum or apt-get enabled).'
                        log.critical(msg)
                        sys.exit(msg)  # write msg and exit with status 1
                                
        verifyPythonDevel()

        if not options.noinstall:                 
                try:
                        installC('yaml', '/usr/local/lib/libyaml.so')
                        import yaml 
                except:
                        log.info("unable to install libyaml, will using pure python version: %s", sys.exc_info()[1])

                try:
                        installPython('PyYAML', 'yaml', 'install')
                except PBuildException:
                        installPython('PyYAML', 'yaml', '--without-libyaml install')  # try without libyaml if build error
        
                log.info("Installing unittest2")
                installPython('unittest2', 'unittest2', 'install')
                log.info("Installing networkx")
                installPython('networkx', 'networkx', 'install')
                magidist = 'MAGI-1'
                log.info("Installing %s", magidist)
                installPython(magidist, 'alwaysinstall', 'install')

                if not options.nodataman:
                        log.info("Installing pymongo")
                        installPython('pymongo', 'pymongo', 'install')
                
                #updating sys.path with the installed packages
                import site
                site.main()

                if not options.nodataman:
                        if is_os_64bit():
                                call("rsync " + rpath + "/" + "mongodb-linux-x86_64-2.4.1.tgz" + " /tmp/mongodb.tgz")
                                call("tar -C /tmp/ -zxvf /tmp/mongodb.tgz")
                                call("sudo rsync /tmp/mongodb-linux-x86_64-2.4.1/bin/* /usr/local/bin/")
                        else:
                                call("rsync " + rpath + "/" + "mongodb-linux-i686-2.4.1.tgz" + " /tmp/mongodb.tgz")
                                call("tar -C /tmp/ -zxvf /tmp/mongodb.tgz")
                                call("sudo rsync /tmp/mongodb-linux-i686-2.4.1/bin/* /usr/local/bin/")

        # Now that MAGI is installed on the local node, import utilities 
        from magi import __version__
        from magi.util import config

        try:
                os.makedirs('/var/log/magi')  # Make sure log directory is around
        except OSError, e:
                if e.errno != errno.EEXIST:
                        log.error("Failed to create logging dir: %s", e, exc_info=1)

        if not options.magiconf:
                log.info("Magi conf not specified on the command line....")
                log.info("Testing to see if one is present....")
                # create a configuration file for magi on the local node only if one is not explicility 
                # specified 
                configFlag = False 
                try: 
                        # Create a magi conf file if it is not present or needs to be recreated 
                        if config.verifyConfig(config.DEFAULT_MAGICONF) == True :
                                log.info("Found a magi conf at %s....", config.DEFAULT_MAGICONF) 
                                if options.force == True:
                                        log.info("Creating a new magi conf due to -f....")  
                                        configFlag  = True 
                                else:
                                        log.info("Using the magi conf at %s....", config.DEFAULT_MAGICONF)
                        
                        else: 
                                # There is not default magi conf or it is not the correct lenght        
                                configFlag = True 
                        
                        if configFlag== True: 
                                log.info("Creating a new magi conf....")
                            
                                # 5/14/2013 The messaging overlay is now explicitly defined in a mesdl configuration file
                                # The mesdl configuration file format is documented in magi/util/config.py
                                # In the absense of a mesdl file, the bootstrap process defines a simple messaging overlay
                                # that starts two servers; one externally facing for the experimenter to connect to the experiment
                                # and one internally facing to forward magi messages to all the experiment nodes. 
                                # It then creates a mesdl file and stores it at DEFAULT_EXPMESDL location 
                                # The node that hosts both the servers is choosen as follows:
                                #    - it checks to see if a node named "control" is present in the experiment 
                                #      If present, it is choosen 
                                #   -  else, the first node in an alphanuremically sorted  
                                #      list of all node names is used  
                                #
                                log.info("Checking to see if a MeSDL file is specified....")
                                if not options.mesdl:
                                        log.info("No Mesdl file specified....")      
                                        options.mesdl = config.createMESDL()
                                        log.info("Created a mesdl file at location %s....",options.mesdl) 
                                else:
                                        log.info("Using MeSDL file at location %s....", options.mesdl)

                                log.info("Checking to see if a db config file is specified....")
                                
                                if not options.nodataman:
                                        if not options.dbconf:
                                                log.info("No db config file specified....")
                                                options.dbconf = config.createDBConf()
                                                log.info("Created a db config file at location %s....",options.dbconf)
                                        else:
                                                log.info("Validating data config file at location %s....", options.dbconf)
                                                options.dbconf = config.validateDBConf(options.dbconf)
                
                                # The mesdl file was specified at the command line 
                                # use it to create a new local node specific magi conf  
                                config.createConfig( mesdl=options.mesdl, dbconf=options.dbconf, rootdir=rpath, enable_dataman=not options.nodataman) 
                                log.info("Created a magi conf at %s....", config.DEFAULT_MAGICONF) 
                except Exception, e:
                        log.error("Magi Config failed, things probably aren't going to run: %s", e, exc_info=True)


        #MongoDB setup
#        from magi.testbed import testbed
#        from magi.util import mongo
#
#        if testbed.nodename == testbed.getServer():
#                mongo.startMongoConfig()
#
#        dbconf = config.getConfig(options.dbconf)
#        if config.getConfig().get('isDBHost'):
#                mongo.startMongoS()
#                mongo.startMongoD()
#                mongo.regsiterMongoDAsShard()
#        elif config.getConfig().get('isCache'):
#                mongo.startMongoD()
#        else:
#                log.info("mongo db not required on this node")

        if not options.nodataman:
                from magi.util import database
                if config.getConfig().get('isDBHost'):
                        database.startDBServer()
                else:
                        log.info("Database server is not required on this node")
                
                
        # Get the pid for the daemon process from the pid file 
        # Note that if the daemon was started externally, without bootstrap, the pid file would not have 
        # the correct pid 
        # Note the magi_daemon.py does record the pid correctly in /var/log/mgi/magi.pid 
        # TODO: walk the /proc directory, check the /proc/pid/status. get the name and kill process if name is magi_daemon.py 
       
        # currenty, check if the magi.pid file exsist, if exists, try to kill  
        # Note the file is created by the magi_daemon.py script  
        log.info("Testing to see if daemon is running....(pid at  %s)", config.DEFAULT_MAGIPID)
        if os.path.exists(config.DEFAULT_MAGIPID) and os.path.getsize(config.DEFAULT_MAGIPID) > 0:
                fpid = open (config.DEFAULT_MAGIPID, 'r')
                pid = int(fpid.read())
                try:
                        os.kill(pid,signal.SIGKILL)
                except OSError, e:
                        if e.args[0] == errno.ESRCH:
                                log.info("No process %d found", pid)
                        else:
                                log.info("Cannot kill process %d", pid) 
        else:
                log.info("No %s file found....", config.DEFAULT_MAGIPID)

        log.info("Starting daemon")
        daemon = ['/usr/local/bin/magi_daemon.py']

        if options.nodataman:
                log.info("Starting daemon without data manager")
                daemon += ['-D']
                
        if options.dargs:
                log.info("Starting daemon with debugging")
                daemon += ['-l', 'magi', '1']
                
                
        # Record the process id in a file for later reference 
        pid=Popen( daemon ).pid
 
        log.info("MAGI Version: %s", __version__) 
        log.info("Started daemon with pid %s", pid)

