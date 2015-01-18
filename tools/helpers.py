#!/usr/bin/env python

import errno
import glob
import logging
from os import path
import os
import signal
from subprocess import Popen
import time


class BootstrapException(Exception): pass
class TarException(BootstrapException): pass
class PBuildException(BootstrapException): pass
class CBuildException(BootstrapException): pass
class PackageInstallException(BootstrapException): pass
class DException(BootstrapException): pass

log = logging.getLogger()

def call(*popenargs, **kwargs):
        log.info("Calling %s" % (popenargs))
        if "shell" not in kwargs:
                kwargs["shell"] = True
        process = Popen(*popenargs, **kwargs)
        process.wait()
        return process.returncode

def installPython(base, check, commands, rpath="/share/magi/current"):
        log.info("Installing %s", base)

        try:
                exec("import " + check)
                log.info("%s already installed, checked with 'import %s'", base, check)
                return
        except ImportError:
                pass
    
        extractDistribution(base, '/tmp', rpath)
        
        distDir = glob.glob(os.path.join("/tmp", base+'*'))[0] # Need glob as os.chdir doesn't expand
        log.info("Changing directory to %s" %(distDir))
        os.chdir(distDir)
        
        if call("python setup.py %s -f" % commands):
                log.error("Failed to install %s with commands %s", base, commands)
                raise PBuildException("Unable to install %s with commands %s" %(base, commands))
            
        log.info("Successfully installed %s", base)

def installC(base, check, rpath="/share/magi/current"):
        log.info("Installing %s", base)
        
        if os.path.exists(check):
                log.info("%s already installed, found file %s", base, check)
                return

        extractDistribution(base, '/tmp', rpath)
        
        distDir = glob.glob(os.path.join("/tmp", base+'*'))[0] # Need glob as os.chdir doesn't expand
        log.info("Changing directory to %s" %(distDir))
        os.chdir(distDir)
        
        if call("./configure") or call("make") or call("make install"):
                log.error("Failed to install %s", base)
                raise CBuildException("Unable to install %s" % base)
            
        log.info("Successfully installed %s", base)

def installPreBuilt(base, rpath="/share/magi/current"):
    log.info("Installing %s", base)
    
    extractDistribution(base, '/tmp', rpath)
            
    distDir = glob.glob(os.path.join("/tmp", base+'*'))[0] # Need glob as os.chdir doesn't expand
    log.info("Changing directory to %s" %(distDir))
    os.chdir(distDir)
    
    if call("sudo rsync bin/* /usr/local/bin/"):
            log.error("Failed to install %s", base)
            raise CBuildException("Unable to install %s" % base)
        
    log.info("Successfully installed %s", base)
        
def extractDistribution(base, destDir, rpath="/share/magi/current"):
    log.info("Extracting %s to %s" %(base, destDir))
    
    paths = [os.path.join(rpath, 'tarfiles', '%s*' %(base)), os.path.join(rpath, '%s*' %(base))]
    fail = False
    for path in paths:
            if call("tar -C %s -xzf %s" % (destDir, path)):
                    fail = True
            else:
                    fail = False
                    break

    if fail:
            raise TarException("Failed to untar %s" % base)
    
    log.info("Successfully extracted %s to %s" %(base, destDir))
    
def isInstalled(program):
        try:
                import subprocess
                subprocess.call(program, stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
                return True
        except:
                return False

def installPackage(yum_pkg_name, apt_pkg_name):
    if isInstalled('yum'):
        log.info("Installing package %s", yum_pkg_name)
        if call("yum install -y %s", yum_pkg_name):
            log.error("Failed to install %s", yum_pkg_name)
            raise PackageInstallException("Unable to install %s" %yum_pkg_name)
        log.info("Successfully installed %s", yum_pkg_name)
    elif isInstalled('apt-get'):
        log.info("Installing package %s", apt_pkg_name)
        if call("apt-get -qq install -y %s" % apt_pkg_name):
            log.error("Failed to install %s", apt_pkg_name)
            raise PackageInstallException("Unable to install %s" %apt_pkg_name)
        log.info("Successfully installed %s", apt_pkg_name)
    
def verifyPythonDevel():
        import distutils.sysconfig as c
        if not os.path.exists(os.path.join(c.get_python_inc(), 'Python.h')):
                try:
                    installPackage(yum_pkg_name="python-devel", apt_pkg_name="python-dev")
                except:
                    pass
        if not os.path.exists(os.path.join(c.get_python_inc(), 'Python.h')):
                log.error("Python development not installed, nor can we use the local package manager to do so")
                
def is_os_64bit():
        import platform
        return platform.machine().endswith('64')

def is_running(pid):        
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
    return True

def terminate(pidFile, timeout=10):
    if os.path.exists(pidFile) and os.path.getsize(pidFile) > 0:
        fpid = open (pidFile, 'r')
        pid = int(fpid.read())
        
        log.info("Process ID %d", pid)
        log.info("Trying to stop process gracefully. Sending SIGTERM.")
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError, e:
            if e.args[0] == errno.ESRCH:
                log.info("No process %d found", pid)
            else:
                log.info("Cannot kill process %d", pid) 
        
        terminated = False
        
        #wait for process to terminate
        for i in range(timeout):
            if is_running(pid):
                time.sleep(0.5)
            else:
                log.info("Process %d terminated successfully", pid)
                terminated = True 
                break
        
        if not terminated:
            log.info("Could not stop process gracefully. Sending SIGKILL.")
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError, e:
                if e.args[0] == errno.ESRCH:
                    log.info("No process %d found", pid)
                else:
                    log.info("Cannot kill process %d", pid) 
        
            #wait for process to die
            for i in range(timeout):
                if is_running(pid):
                    time.sleep(0.5)
                else:
                    log.info("Process %d killed successfully", pid)
                    terminated = True 
                    break
            
        return terminated
    
    else:
        log.info("No %s file found", pidFile)
        return True
    