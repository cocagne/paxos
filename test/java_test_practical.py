#!/usr/bin/env jython

import sys
import os.path
import unittest

#sys.path.append
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.join(os.path.dirname(this_dir),'bin') )

from cocagne.paxos.essential import ProposalID
from cocagne.paxos import practical, essential

import test_essential
import test_practical
import java_test_essential

from java_test_essential import PID

def JPID(pid):
    return essential.ProposalID(pid.number, pid.uid) if pid is not None else None


class MessengerAdapter (java_test_essential.MessengerAdapter):

    def sendPrepareNACK(self, proposerUID, proposalID, promisedID):
        self.send_prepare_nack(proposerUID, PID(proposalID), PID(promisedID))

    def sendAcceptNACK(self, proposerUID, proposalID, promisedID):
        self.send_accept_nack(proposerUID, PID(proposalID), PID(promisedID))

    def onLeadershipAcquired(self):
        self.on_leadership_acquired()


class PracticalMessengerAdapter(MessengerAdapter, practical.PracticalMessenger, test_practical.PracticalMessenger):
    pass





class GenericProposerAdapter(java_test_essential.ProposerAdapter):

    @property
    def leader(self):
        return self.isLeader()

    @leader.setter
    def leader(self, value):
        self.setLeader(value)

    @property
    def active(self):
        return self.isActive()

    @active.setter
    def active(self, value):
        self.setActive(value)

    def observe_proposal(self, from_uid, proposal_id):
        self.observeProposal(from_uid, JPID(proposal_id))

    def recv_prepare_nack(self, from_uid, proposal_id, promised_id):
        self.receivePrepareNACK(from_uid, JPID(proposal_id), JPID(promised_id))

    def recv_accept_nack(self, from_uid, proposal_id, promised_id):
        self.receiveAcceptNACK(from_uid, JPID(proposal_id), JPID(promised_id))

    def resend_accept(self):
        self.resendAccept()


class ProposerAdapter(practical.PracticalProposerImpl, GenericProposerAdapter):
    pass

class NodeProposerAdapter(practical.PracticalNode, GenericProposerAdapter):
    pass


class GenericAcceptorAdapter(test_practical.AutoSaveMixin, java_test_essential.AcceptorAdapter):

    @property
    def active(self):
        return self.isActive()

    @active.setter
    def active(self, value):
        self.setActive(value)

    @property
    def persistance_required(self):
        return self.persistenceRequired()

    #def recover(self, promised_id, accepted_id, accepted_value):
    #    practical.PracticalAcceptor.recover(self, JPID(promised_id), JPID(accepted_id), accepted_value)


class AcceptorAdapter(practical.PracticalAcceptorImpl, GenericAcceptorAdapter):
    def recover(self, promised_id, accepted_id, accepted_value):
        practical.PracticalAcceptor.recover(self, JPID(promised_id), JPID(accepted_id), accepted_value)


class NodeAcceptorAdapter(practical.PracticalNode, GenericAcceptorAdapter):
    def recover(self, promised_id, accepted_id, accepted_value):
        practical.PracticalNode.recover(self, JPID(promised_id), JPID(accepted_id), accepted_value)


class PracticalProposerTester(test_practical.PracticalProposerTests, PracticalMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def proposer_factory(self, messenger, uid, quorum_size):
        return ProposerAdapter(messenger, uid, quorum_size)


class PracticalAcceptorTester(test_practical.PracticalAcceptorTests, PracticalMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def acceptor_factory(self, messenger, uid, quorum_size):
        return AcceptorAdapter(messenger)


class PracticalNodeProposerTester(test_practical.PracticalProposerTests, PracticalMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def proposer_factory(self, messenger, uid, quorum_size):
        return NodeProposerAdapter(messenger, uid, quorum_size)


class PracticalNodeAcceptorTester(test_practical.PracticalAcceptorTests, PracticalMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def acceptor_factory(self, messenger, uid, quorum_size):
        return NodeAcceptorAdapter(messenger, uid, quorum_size)


    

if __name__ == '__main__':
    unittest.main()
