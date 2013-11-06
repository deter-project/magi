# Copyright (C) 2012 University of Southern California
# This software is licensed under the GPLv3 license, included in
# ./GPLv3-LICENSE.txt in the source distribution

from emulab import EmulabTestbed

class FakeEmulabTestbed(EmulabTestbed):

    def __init__(self, **hint): EmulabTestbed.__init__(self, **hint)
    def getSwapperData(self): return "CREATOR=bwilson SWAPPER=bwilson"
    def getNicknameData(self): return "c0.large.FloodWatch"
    def getControlIfData(self): return "eth4"
    def getIfconfigData(self): return ifconfigdata
    def getHostForName(self, name): return "192.168.1.1"
    def getIfFor(self, inet, mac): return "eth0"


ifconfigdata = [
"INTERFACE IFACETYPE=em INET=100.100.249.2 MASK=255.255.255.0 MAC=0015175d13d8 SPEED=100Mbps DUPLEX=full IFACE= RTABID=0 LAN=lanc50",
"INTERFACE IFACETYPE=em INET=100.100.254.1 MASK=255.255.255.0 MAC=0015175d13d9 SPEED=100Mbps DUPLEX=full IFACE= RTABID=0 LAN=lanc01",
"INTERFACE IFACETYPE=em INET=100.100.191.2 MASK=255.255.255.0 MAC=0015175d13da SPEED=100Mbps DUPLEX=full IFACE= RTABID=0 LAN=lancr0",
]

