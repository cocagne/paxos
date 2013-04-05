#!/usr/bin/env jython

import sys
import os.path
import unittest

#sys.path.append
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.join(os.path.dirname(this_dir),'bin') )

from cocagne.paxos.essential import ProposalID
from cocagne.paxos import essential

import test_essential

def PID(proposalID):
    if proposalID is not None:
        return test_essential.PID(proposalID.getNumber(), proposalID.getUID())

class MessengerAdapter (object):

    def sendPrepare(self, proposalID):
        self.send_prepare(PID(proposalID))

    def sendPromise(self, proposerUID, proposalID, previousID, acceptedValue):
        self.send_promise(proposerUID, PID(proposalID), PID(previousID), acceptedValue)

    def sendAccept(self, proposalID, proposalValue):
        self.send_accept(PID(proposalID), proposalValue)

    def sendAccepted(self, proposalID, acceptedValue):
        self.send_accepted(PID(proposalID), acceptedValue)
    
    def onResolution(self, proposalID, value):
        self.on_resolution(PID(proposalID), value)


class EssentialMessengerAdapter(MessengerAdapter, essential.EssentialMessenger, test_essential.EssentialMessenger):
    pass



class ProposerAdapter(object):

    @property
    def proposer_uid(self):
        return self.getProposerUID()

    @property
    def quorum_size(self):
        return self.getQuorumSize()
    
    @property
    def proposed_value(self):
        return self.getProposedValue()
    
    @property
    def proposal_id(self):
        return PID(self.getProposalID())

    @property
    def last_accepted_id(self):
        return PID(self.getLastAcceptedID())

    def set_proposal(self, value):
        self.setProposal(value)
        
    def recv_promise(self, from_uid, proposal_id, prev_accepted_id, prev_accepted_value):
        if prev_accepted_id is not None:
            prev_accepted_id = essential.ProposalID(prev_accepted_id.number, prev_accepted_id.uid)

        self.receivePromise(from_uid, essential.ProposalID(proposal_id.number, proposal_id.uid),
                            prev_accepted_id, prev_accepted_value)


        
class AcceptorAdapter(object):

    @property
    def promised_id(self):
        return PID(self.getPromisedID())

    @property
    def accepted_id(self):
        return PID(self.getAcceptedID())

    @property
    def accepted_value(self):
        return self.getAcceptedValue()

    def recv_prepare(self, from_uid, proposal_id):
        self.receivePrepare(from_uid, essential.ProposalID(proposal_id.number, proposal_id.uid))

    def recv_accept_request(self, from_uid, proposal_id, value):
        self.receiveAcceptRequest(from_uid, essential.ProposalID(proposal_id.number, proposal_id.uid), value)



class LearnerAdapter(object):

    @property
    def quorum_size(self):
        return self.getQuorumSize()

    @property
    def complete(self):
        return self.isComplete()

    @property
    def final_value(self):
        return self.getFinalValue()

    @property
    def final_proposal_id(self):
        return PID(self.getFinalProposalID())

    def recv_accepted(self, from_uid, proposal_id, accepted_value):
        self.receiveAccepted(from_uid,
                             essential.ProposalID(proposal_id.number, proposal_id.uid),
                             accepted_value)

        
class EssentialProposerAdapter(essential.EssentialProposerImpl, ProposerAdapter):
    pass

class EssentialAcceptorAdapter(essential.EssentialAcceptorImpl, AcceptorAdapter):
    pass

class EssentialLearnerAdapter(essential.EssentialLearnerImpl, LearnerAdapter):
    pass

        
class EssentialProposerTester(test_essential.EssentialProposerTests, EssentialMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def proposer_factory(self, messenger, uid, quorum_size):
        return EssentialProposerAdapter(messenger, uid, quorum_size)


class EssentialAcceptorTester(test_essential.EssentialAcceptorTests, EssentialMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def acceptor_factory(self, messenger, uid, quorum_size):
        return EssentialAcceptorAdapter(messenger)


class EssentialLearnerTester(test_essential.EssentialLearnerTests, EssentialMessengerAdapter, unittest.TestCase):

    def __init__(self, test_name):
        unittest.TestCase.__init__(self, test_name)
        
    def learner_factory(self, messenger, uid, quorum_size):
        return EssentialLearnerAdapter(messenger, quorum_size)



    
if __name__ == '__main__':
    unittest.main()
