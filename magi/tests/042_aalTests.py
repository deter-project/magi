#!/usr/bin/env python

import unittest2
import logging
import os
import pdb

from magi.tests.util import *
from magi.messaging.api import MAGIMessage
from magi.orchestrator.parse import AAL
from magi.orchestrator.orchestrator import Orchestrator

class AALTest(unittest2.TestCase):
    """
        Testing of parsing and running for orchestrator
    """

    def setUp(self):
        self.aal = AAL([os.path.join(os.path.dirname(__file__), 'test.aal')])
        self.messaging = SimpleMessaging()

    #def tearDown(self):

    def test_1Parse(self):
        """ Test parsing of AAL file """
        #TODO: Test needs to be fixed
        return
        
        self.assertEquals(['main', 'branch2'], self.aal.getStartKeys())
        self.assertEquals(16, len(self.aal.getSetupStream()))
        self.assertEquals(5, len(self.aal.getStream('main')))
        self.assertEquals(3, len(self.aal.getStream('branch1')))
        self.assertEquals(2, len(self.aal.getStream('branch2')))
        self.assertEquals(['Node1'], self.aal.getStream('main')[1].args['servers'])
        self.assertEquals(['Node2', 'Node3', 'Node4'], self.aal.getStream('branch1')[0].args['clients'])


    def _verifyMessageCount(self, count):
        self.assertEquals(self.messaging.outgoing.qsize(), count)
        for ii in range(count):
            self.messaging.extract(False)

    def _sendTrigger(self, **data):
        self.messaging.inject(MAGIMessage(groups="control", docks="control", data=yaml.dump(data.copy())))

    def test_2Execution(self):
        """ Test execution of AAL file """
        #TODO: Test needs to be fixed
        return
    
        orch = Orchestrator(self.messaging, self.aal)
        thread = orch.runInThread()
        time.sleep(0.5)

        self._verifyMessageCount(3) # GroupBuild

        self._sendTrigger(event='GroupBuildDone', group='agroup', nodes=['Node3', 'Node2', 'Node1', 'Node4'])
        self._sendTrigger(event='GroupBuildDone', group='Server1-group', nodes=['Node1'])
        self._sendTrigger(event='GroupBuildDone', group='Client1-group', nodes=['Node2'])
        self._sendTrigger(event='GroupBuildDone', group='Client1-group', nodes=['Node3', 'Node1', 'Node4']) # test merge
        time.sleep(10)

        self._verifyMessageCount(5) # LoadAgent

        for agent in ['nothing', 'logger', 'allcounters', 'Server1', 'Client1']:
            # send all nodes, subset should match
            self._sendTrigger(event='AgentLoadDone', name=agent, nodes=['Node1', 'Node2', 'Node3', 'Node4']) 

        time.sleep(0.5)

        self._verifyMessageCount(4) # 4 commands before timeout

        time.sleep(3.0)

        self._verifyMessageCount(1) # 1 command after timeout, before next trigger

        self._sendTrigger(event='gobranch1', special1='matchme')
        time.sleep(0.5) # should trigger, send 1 message and wait on a 1 second timeout

        self._verifyMessageCount(1) 
        time.sleep(0.8) 
        self._verifyMessageCount(2) 

        thread.stop()
        time.sleep(0.1)

    def test_returnFalse(self):
        #TODO: Test needs to be fixed
        return
        
        orch = Orchestrator(self.messaging, self.aal)
        thread = orch.runInThread()
        time.sleep(0.5)
        self._sendTrigger(event='someMethodCall', group='someGroup', nodes=['one'], agent='someAgent', result=[False])
        time.sleep(5)

    def test_runtimeException(self):
        #TODO: Test needs to be fixed
        return
        
        orch = Orchestrator(self.messaging, self.aal)
        thread = orch.runInThread()
        time.sleep(0.5)
        self._sendTrigger(event='RuntimeException', group='someGroup', nodes=['one'], agent='someAgent')
        time.sleep(5)

if __name__ == '__main__':
    import signal
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    hdlr = logging.StreamHandler()
    hdlr.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%m-%d %H:%M:%S'))
    root = logging.getLogger()
    root.handlers = []
    root.addHandler(hdlr)
    root.setLevel(logging.DEBUG)
    unittest2.main(verbosity=2)


