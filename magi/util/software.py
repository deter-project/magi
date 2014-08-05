#!/usr/bin/env python

import os, glob, logging, tempfile, errno, datetime
import tarfile, zipfile, subprocess, threading, platform
import re
from os import mkdir
from magi.util.execl import run, execAndRead
from magi.util.config import getConfig

log = logging.getLogger(__name__)

if 'installedList' not in locals():
    installedList = set()  # do we want a global, this feels icky
    installLock = threading.RLock()
    installDisable = False

def requireSoftware(*args, **kwargs):
    """
        Called by daemon or modules to confirm installation of software
        Each unnamed argument is a string indicating software to look for.  The function will attempt to match the first argument
        and if it can't find it, it will attempt the next argument.  This multiple argument options is provided for software that can
        be present under different names such as apache2 or httpd.

        The installer list is loaded from the node configuration and might look something like so:

         * AptInstaller
         * ArchiveInstaller(dir="/share/magi/v20/Linux-Ubuntu10.04-i686")
         * SourceInstaller(dir="/share/magi/v20/source")


        Optional kwargs arguments:
         * os = (Linux, FreeBSD, etc), only use on a matching OS name
         * force = If True, force installation. 
         * update = If True, update the package manager before installing the software. This only makes sense on package manager-based installers. 
         * upgrade = If True, upgrade the entire system prior to installing the software. This only makes sense on package manager-based installers. 

    """
    global installedList
    global installDisable
    global installLock

    if installDisable:
        log.debug("installer disabled, not installing %s", str(args))
        return

    if 'os' in kwargs:
        if str(kwargs['os']).lower() != platform.system().lower():
            log.debug("Skipping require %s as %s != %s", args, kwargs['os'], platform.system())
            return

    installLock.acquire()
    try:
        # Check if we already installed it
        for name in args:
            if name in installedList:
                log.info("%s is already installed, skipping", name)
                return

        # Build a new list of potential installers from config (this will reread conf each call for up to date values)
        config = getConfig()
        installerList = list()
        for item in config['software']:
            # Call appropriate constructor with item dict as kwargs argument
            try:
                installerList.append({
                        AptInstaller.TYPE: AptInstaller,
                        YumInstaller.TYPE: YumInstaller,
                        RPMFileInstaller.TYPE: RPMFileInstaller,
                        ArchiveInstaller.TYPE: ArchiveInstaller,
                        SourceInstaller.TYPE: SourceInstaller
                }[item['type']](**item))
            except KeyError:
                log.error("Ignoring unknown installer type %s", item.get('type', 'No Type'))

        # Now actually try and install things using the given installers
        for installer in installerList:

            # invoke pre-install commands, if wanted
            # Note: order is important: update before upgrade
            for command in ['update', 'upgrade']:
                if command in kwargs and kwargs[command]:
                    getattr(installer, command)()

            for name in args:
                log.info('Attempting install %s with %s' % (name, installer.__class__.__name__))
                if installer.install(name, **kwargs):
                    log.info("Satisfied request for %s with %s", args, installer.__class__.__name__)
                    installedList.add(name)
                    # we reload sys as the install may have appended to sys.path if we're installing
                    # oddball python packages. The agent will not be able to find them otherwise.
                    import sys
                    reload(sys)
                    return

        # If we get here, nothing worked, let them know
        log.error("Unable to install %s", args)

    finally:
        installLock.release()
    
class Installer(object):
    TYPE='None'
    def __init__(self, **kwargs):
        self.dir = "nodirspecified"
        self.__dict__.update(kwargs)
        self.log = logging.getLogger(self.TYPE)

    def locate(self, dirpath, name, extension):
        match = glob.glob(os.path.join(dirpath, name+'*'+extension))
        error = errno.ENOENT
        if len(match) > 0:
            if os.access(match[0], os.R_OK):  # On cygwin, no X, only R so we only return R or not
                return match[0]
            error = errno.EPERM
        raise OSError(error, os.strerror(error))

    def update(self): 
        '''Update the install/package manager. This is not a system upgrade. This may not make sense for non-package manager based installers.'''
        self.log.warn('Update not supported for this install type.')
        pass

    def upgrade(self): 
        '''Upgrade the system using the package manager. This may not make sense for non-package manager based installers.'''
        self.log.warn('Upgrade not supported for this install type.')
        pass

    def doAndLogCommand(self, command, name):
        '''Run the given command and dump stdout and stderr to /var/log/magi/.'''
        # TODO: replace hardcoded file name with magi.util.config.MAGILOG 
        filename = '/var/log/magi/%s-%s.log' % (self.TYPE, name)
        try:
            with open(filename, 'a') as fd: 
                return run(command, stdout=fd, stderr=subprocess.STDOUT, shell=True) == 0
        except Exception, e:
            self.log.warning('%s failed %s: %s. Check %s for details.', self.TYPE, name, e, filename)
            return False

class AptInstaller(Installer):
    """
        AptInstaller makes use of the apt-get command on Debian/Ubuntu systems.  It returns
        true if the software is sucessfully installed or already installed
    """
    TYPE="apt"
    def install(self, name, **kwargs):
        cmd = 'DEBIAN_FRONTEND=noninteractive apt-get install %s --assume-yes' % name
        if 'force' in kwargs and kwargs['force']:
            cmd += ' --force-yes'

        retVal = self.doAndLogCommand(cmd, 'install')
        if not retVal:
            log.info('Unable to install using apt-get most likely because this package is not in the packet repo.')

        return retVal

    def update(self):
        '''resynchronize the package index files from their sources.'''
        return self.doAndLogCommand('DEBIAN_FRONTEND=noninteractive apt-get update -y', 'update')
            
    def upgrade(self):
        '''Upgrade the system via apt-get.'''
        return self.doAndLogCommand('DEBIAN_FRONTEND=noninteractive apt-get upgrade -y', 'upgrade')

class YumInstaller(Installer):
    """
        YumInstaller makes use of the yum command on Redhat/Fedora/CentOS systems.  It returns
        true if the software is sucessfully installed or already installed.
    """
    TYPE="yum"
    def install(self, name, **kwargs):
        try:
            (output, error) = execAndRead("yum install -y %s" % name)
            for line in output.split('\n') + error.split('\n'):
                if re.search("No package %s available" % name, line):
                    return False
                if re.search("No Match for argument: %s" % name, line):
                    return False
            return True
        except Exception, e:
            self.log.warning("yum failed: %s", e)
            return False

    def update(self):
        '''Update yum itself. There is not really an update concept in yum, at least not how it is meant here.'''
        return self.doAndLogCommand('yum update yum --assumeyes', 'update')

    def upgrade(self):
        '''Upgrade the system via yum.'''
        return self.doAndLogCommand('yum upgrade yum --assumeyes', 'upgrade')

class RPMFileInstaller(Installer):
    """
        RPMFileInstaller uses the rpm command to install a single rpm file.  It returns
        true if the command returns without an error, false otherwise.
        It searches the directory passed in as a keyword "dir=X"
    """
    TYPE="rpmfile"
    def install(self, name, **kwargs):
        try: 
            cmd = 'rpm -i %s' % self.locate(self.dir, name, '.rpm')
        except OSError, e:
            d = self.dir if self.dir else "None"
            self.log.warning('Error installing via rpm from dir %s: %s' % (d, e))
            return False

        return self.doAndLogCommand(cmd)

class ArchiveInstaller(Installer):
    """
        ArchiveInstaller looks for .t*z files or .zip files and extracts them onto the local
        node into the root directory.  It returns true if an archive is found and the extract
        command doesn't throw any errors.
        It searches the directory passed in as a keyword "dir=X"
    """
    TYPE="archive"
    def install(self, name, **kwargs):
        try:
            self.tarInstall(self.locate(self.dir, name, '.t*z'))   # .tgz or .tar.gz
            return True
        except Exception, e:
            self.log.debug("Unable to install %s as tarfile: %s", name, e)

        try:
            self.zipInstall(self.locate(self.dir, name, '.zip'))
            return True
        except:
            self.log.debug("Unable to install %s as zipfile: %s", name, e)

        return False

    def tarInstall(self, filepath):
        tar = tarfile.open(filepath, "r")
        for m in tar.getmembers():
            tar.extract(m, '/')
        files = tar.getnames()
        tar.close()

    def zipInstall(self, filepath):
        archive = zipfile.ZipFile(filepath, "r")
        fileinfo = archive.infolist()
        files = archive.namelist()
        for f in fileinfo:
            outfile = None
            fileperm = ((f.external_attr >> 16L) & 0777)
            if f.internal_attr == 0:
                try:
                    mkdir('/'+f.filename)
                except Exception:
                    pass
            elif f.internal_attr > 0:
                try:
                    outfile = os.open('/'+f.filename, os.O_CREAT | os.O_WRONLY, fileperm)
                    os.write(outfile, archive.read(f.filename))
                    os.close(outfile)
                except Exception, e:
                    self.log.warning("Error while unzipping %s : %s (%s)" % (self.name, f.filename, e))
                    return False



class SourceInstaller(Installer):
    """
        SourceInstaller looks for a script with the requested software name and runs it.
        It returns true if the script is found and returns a value of 0, false otherwise.
        It searches the directory passed in as a keyword "dir=X".
        The script is called with the arguments [temp directory to build in, directory of script file]
    """
    TYPE="source"
    def install(self, name, **kwargs):
        """ Try and build the package using a user provided script """
        try:
            ret = True
            log.debug('looking for install script named %s.install in dir %s' % (name, self.dir))
            buildscript = self.locate(self.dir, name, '.install')
            builddir = "/tmp" #tempfile.mkdtemp()
            cmd = "%s %s %s" % (buildscript, builddir, os.path.abspath(self.dir))
                
            for k,v in kwargs.iteritems():
                cmd+=' %s=%s' % (k,v)

        except Exception, e:
            self.log.debug("Unable to build %s from source: %s" % (name, e))
            return False

        return self.doAndLogCommand(cmd, "install")

