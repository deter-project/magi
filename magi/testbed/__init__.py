import os

from magi.util import helpers

import base
import containers
import desktop 
import emulab
import fakeemulab

# The use of TestbedContainer is to create a layer of indirection, which helps
# us modify the underlying testbed class and make sure that all the existing 
# references would also point to the new class
class TestbedContainer:
    def __init__(self):
        ## Default init is an emulab testbed, user can override
        if os.path.exists('/var/containers'):
            self.contained = containers.ContainerExperiment()
        elif os.path.exists('/var/emulab/boot/nickname'):
            self.contained = emulab.EmulabTestbed()
        else:
            self.contained = desktop.DesktopExperiment()
    
    def __getattr__(self, object):
        return getattr(self.contained, object)
    
    def getTestbedClassFQCN(self):
        return helpers.getFQCN(self.contained)
    
    def setTestbedClass(self, fqcn):
        if not fqcn.startswith('magi.testbed.'):
            raise AttributeError('Invalid testbed class name: %s' %fqcn)
        self.contained = helpers.createClassInstance(fqcn)
    
    def getTestbedClassInstance(self):
        return self.contained
    
# This is to make sure that only one instance of testbed exists        
try:
    testbed
except NameError:
    testbed = TestbedContainer()