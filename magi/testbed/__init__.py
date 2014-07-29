
import os
import base
import emulab
import containers
import fakeemulab
import desktop 

class Proxy:
    def __init__(self, subject): self.setSubject(subject)
    def setSubject(self, subject): self.__subject = subject
    def __getattr__(self, name): return getattr(self.__subject, name)  

def initEmulabTestbed():
    testbed.setSubject(emulab.EmulabTestbed())

def initFakeTestbed():
    testbed.setSubject(fakeemulab.FakeEmulabTestbed())

## Default init is an emulab testbed, user can override
if os.path.exists('/var/containers'):
    testbed = Proxy(containers.ContainerExperiment())
elif os.path.exists('/var/emulab/boot/nickname'):
    testbed = Proxy(emulab.EmulabTestbed())
else:
    testbed = Proxy(desktop.DesktopExperiment())

