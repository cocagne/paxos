#!/usr/bin/env jython

import sys
import os.path
import unittest

#sys.path.append
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.join(os.path.dirname(this_dir),'bin') )

from cocagne.paxos.essential import ProposalID
from cocagne.paxos import essential, practical, functional

import test_essential
import test_practical
import test_functional
import java_test_essential
import java_test_practical

from java_test_essential import PID


def JPID(pid):
    return essential.ProposalID(pid.number, pid.uid) if pid is not None else None


class MessengerAdapter (java_test_practical.MessengerAdapter):

    def sendPrepareNACK(self, proposerUID, proposalID, promisedID):
        self.send_prepare_nack(proposerUID, PID(proposalID), PID(promisedID))

    def sendAcceptNACK(self, proposerUID, proposalID, promisedID):
        self.send_accept_nack(proposerUID, PID(proposalID), PID(promisedID))

    def onLeadershipAcquired(self):
        self.on_leadership_acquired()

    def sendHeartbeat(self, leaderProposalID):
        self.send_heartbeat(leaderProposalID)

    def schedule(self, millisecondDelay, callback):
        self._do_schedule(millisecondDelay, lambda : callback.execute())

    def onLeadershipLost(self):
        self.on_leadership_lost()

    def onLeadershipChange(self, previousLeaderUID, newLeaderUID):
        self.on_leadership_change(previousLeaderUID, newLeaderUID)


class HeartbeatMessengerAdapter(MessengerAdapter, functional.HeartbeatMessenger, test_functional.HeartbeatMessenger):
    def _do_schedule(self, ms, cb):
        test_functional.HeartbeatMessenger.schedule(self, ms, cb)


class HNode(functional.HeartbeatNode, java_test_practical.GenericProposerAdapter, java_test_practical.GenericAcceptorAdapter, java_test_essential.LearnerAdapter):
    hb_period       = 2
    liveness_window = 6

    def __init__(self, messenger, uid, quorum):
        functional.HeartbeatNode.__init__(self, messenger, uid, quorum, None, 2, 6)

    @property
    def leader_uid(self):
        return self.getLeaderUID()

    @property
    def proposal_id(self):
        pid = PID(self.getProposalID())
        if pid is not None and pid.number == 0:
            return None
        return pid

    @property
    def leader_proposal_id(self):
        return PID(self.getLeaderProposalID())

    @leader_proposal_id.setter
    def leader_proposal_id(self, value):
        self.setLeaderProposalID( JPID(value) )

    @property
    def _acquiring(self):
        return self.isAcquiringLeadership()

    def timestamp(self):
        return self.messenger.timestamp()

    def leader_is_alive(self):
        return self.leaderIsAlive()

    def observed_recent_prepare(self):
        return self.observedRecentPrepare()
    
    def poll_liveness(self):
        self.pollLiveness()

    def recv_heartbeat(self, from_uid, proposal_id):
        self.receiveHeartbeat(from_uid, JPID(proposal_id))

    def acquire_leadership(self):
        self.acquireLeadership()


class HeartbeatTester(test_functional.HeartbeatTests, HeartbeatMessengerAdapter, unittest.TestCase):
    node_factory = HNode

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)

    def setUp(self):
        super(HeartbeatTester,self).setUp()
        self.msetup()
        self.create_node()



class NodeProposerAdapter(functional.HeartbeatNode, java_test_practical.GenericProposerAdapter):
    pass

class NodeAcceptorAdapter(functional.HeartbeatNode, java_test_practical.GenericAcceptorAdapter):
    def recover(self, promised_id, accepted_id, accepted_value):
        functional.HeartbeatNode.recover(self, JPID(promised_id), JPID(accepted_id), accepted_value)

class NodeLearnerAdapter(functional.HeartbeatNode, java_test_essential.LearnerAdapter):
    pass


class HeartbeatNodeProposerTester(test_practical.PracticalProposerTests, HeartbeatMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def proposer_factory(self, messenger, uid, quorum_size):
        return NodeProposerAdapter(messenger, uid, quorum_size, None, 1, 5)

    def setUp(self):
        super(HeartbeatNodeProposerTester,self).setUp()
        self.msetup()

class HeartbeatNodeAcceptorTester(test_practical.PracticalAcceptorTests, HeartbeatMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def acceptor_factory(self, messenger, uid, quorum_size):
        return NodeAcceptorAdapter(messenger, uid, quorum_size, None, 1, 5)

    def setUp(self):
        super(HeartbeatNodeAcceptorTester,self).setUp()
        self.msetup()

class HeartbeatNodeLearnerTester(test_practical.PracticalLearnerTests, HeartbeatMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def learner_factory(self, messenger, uid, quorum_size):
        return NodeLearnerAdapter(messenger, uid, quorum_size, None, 1, 5)

    def setUp(self):
        super(HeartbeatNodeLearnerTester,self).setUp()
        self.msetup()


if __name__ == '__main__':
    unittest.main()

