import os

import base
import containers
import desktop 
import emulab
import fakeemulab


## Default init is an emulab testbed, user can override
if os.path.exists('/var/containers'):
    testbed = containers.ContainerExperiment()
elif os.path.exists('/var/emulab/boot/nickname'):
    testbed = emulab.EmulabTestbed()
else:
    testbed = desktop.DesktopExperiment()

