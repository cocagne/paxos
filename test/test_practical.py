import sys
import itertools
import os.path
import pickle

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import practical

import test_essential 
from   test_essential import PID



class PracticalMessenger (test_essential.EssentialMessenger):
    leader_acquired = False

    def send_prepare_nack(self, to_uid, proposal_id, promised_id):
        self._append('prepare_nack', to_uid, proposal_id, promised_id)

    def send_accept_nack(self, to_uid, proposal_id, promised_id):
        self._append('accept_nack', to_uid, proposal_id, promised_id)

    def on_leadership_acquired(self):
        self.leader_acquired = True


        
class PracticalProposerTests (test_essential.EssentialProposerTests):
    
    def set_leader(self):
        self.p.prepare()
        self.am('prepare', PID(1, 'A'))
        self.p.leader = True
        self.ae( self.p.proposed_value, None )

        
    def test_set_proposal_no_previous_value_as_leader(self):
        self.set_leader()
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.am('accept', PID(1,'A'), 'foo')


    def test_prepare_increment_on_foriegn_promise(self):
        self.p.recv_promise('B', PID(5,'C'), None, None)
        self.p.prepare()
        self.am('prepare', PID(6,'A'))


    def test_preare_no_increment(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise('B', PID(1,'A'), None, None)
        self.ae( len(self.p.promises_rcvd), 1 )
        self.p.prepare(False)
        self.am('prepare', PID(1,'A'))
        self.ae( len(self.p.promises_rcvd), 1 )
        
        
    def test_recv_promise_ignore_when_leader(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.leader = True
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 0 )

        
    def test_recv_promise_acquire_leadership_with_proposal(self):
        self.p.set_proposal('foo')
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.recv_promise( 'C', PID(1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 2 )
        self.at(self.p.leader)
        self.at(self.leader_acquired)
        self.am('accept', PID(1,'A'), 'foo')


    def test_recv_promise_acquire_leadership_without_proposal(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.recv_promise( 'C', PID(1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 2 )
        self.at(self.p.leader)
        self.at(self.leader_acquired)
        self.an()

        
    def test_resend_accept(self):
        self.p.resend_accept()
        self.an()
        self.set_leader()
        self.p.resend_accept()
        self.an()
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.am('accept', PID(1,'A'), 'foo')
        self.p.resend_accept()
        self.am('accept', PID(1,'A'), 'foo')


    def test_observe_proposal(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.p.observe_proposal( 'B', PID(5,'B') )
        self.p.prepare()
        self.am('prepare', PID(6,'A'))


    def test_recv_prepare_nack(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.p.recv_prepare_nack( 'B', PID(1,'A'), PID(5,'B') )
        self.p.prepare()
        self.am('prepare', PID(6,'A'))



class PracticalAcceptorTests (test_essential.EssentialAcceptorTests):
    

    def test_recv_prepare_nack(self):
        self.a.recv_prepare( 'A', PID(2,'A') )
        self.am('promise', 'A', PID(2,'A'), None, None)
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('prepare_nack', 'A', PID(1,'A'), PID(2,'A'))


    def test_recv_accept_request_less_than_promised(self):
        super(PracticalAcceptorTests, self).test_recv_accept_request_less_than_promised()
        self.am('accept_nack', 'A', PID(1,'A'), PID(5,'A'))
        


class PracticalLearnerTests (test_essential.EssentialLearnerTests):
    pass


    
class NodeTester(PracticalMessenger, unittest.TestCase):

    def XXXtest_durability(self):
        n = node.Node(self, 'A', 2)

        n.prepare()
        n.recv_prepare( 'A', (2,'A') )
        n.recv_accept_request( 'A', (2,'A'), 'foo' )
        n.recv_accepted( 'A', (2,'A'), 'foo' )

        self.clear_msgs()
        
        n2 = pickle.loads( pickle.dumps(n) )
        n2.recover(self)

        n2.prepare()
        self.am('prepare', (2,'A'))

        n2.recv_accept_request( 'A', (1,'A'), 'blah' )
        self.am('accept_nack', 'A', (1,'A'), (2,'A'))

        self.at( not n2.complete )
        n2.recv_accepted( 'B', (2,'A'), 'foo' )

        self.at( n2.complete )


    def test_change_quorum_size(self):
        n = practical.Node(self, 'A', 2)
        self.ae(n.quorum_size, 2)
        n.change_quorum_size(3)
        self.ae(n.quorum_size, 3)
        


class AutoSaveMixin(object):
    
    def recv_prepare(self, from_uid, proposal_id):
        super(AutoSaveMixin, self).recv_prepare(from_uid, proposal_id)
        if self.state_save_required:
            self.state_saved()

    def recv_accept_request(self, from_uid, proposal_id, value):
        super(AutoSaveMixin, self).recv_accept_request(from_uid, proposal_id, value)
        if self.state_save_required:
            self.state_saved()

class TNode(AutoSaveMixin, practical.Node):
    pass
    
class PracticalProposerTester(PracticalProposerTests, PracticalMessenger, unittest.TestCase):
    proposer_factory = TNode

class PracticalAcceptorTester(PracticalAcceptorTests, PracticalMessenger, unittest.TestCase):
    acceptor_factory = TNode

class PracticalLearnerTester(PracticalLearnerTests, PracticalMessenger, unittest.TestCase):
    learner_factory = TNode
