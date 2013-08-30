import sys
import itertools
import os.path
import pickle

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


    def test_resend_accept(self):
        self.set_leader()
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.am('accept', PID(1,'A'), 'foo')
        self.p.resend_accept()
        self.am('accept', PID(1,'A'), 'foo')


    def test_resend_accept_not_active(self):
        self.set_leader()
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.am('accept', PID(1,'A'), 'foo')
        self.p.active = False
        self.p.resend_accept()
        self.an()

        
    def test_set_proposal_no_previous_value_as_leader_not_active(self):
        self.set_leader()
        self.p.active = False
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.an()


    def test_prepare_increment_on_foriegn_promise(self):
        self.p.recv_promise('B', PID(5,'C'), None, None)
        self.p.prepare()
        self.am('prepare', PID(6,'A'))

        
    def test_prepare_increment_on_foriegn_promise_not_active(self):
        self.p.active = False
        self.p.recv_promise('B', PID(5,'C'), None, None)
        self.p.prepare()
        self.an()


    def test_preare_no_increment(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise('B', PID(1,'A'), None, None)
        self.ae( self.num_promises(), 1 )
        self.p.prepare(False)
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 1 )
        
        
    def test_recv_promise_ignore_when_leader(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.leader = True
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 0 )

        
    def test_recv_promise_acquire_leadership_with_proposal(self):
        self.p.set_proposal('foo')
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.recv_promise( 'C', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 2 )
        self.at(self.p.leader)
        self.at(self.leader_acquired)
        self.am('accept', PID(1,'A'), 'foo')


    def test_recv_promise_acquire_leadership_with_proposal_not_active(self):
        self.p.set_proposal('foo')
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.active = False
        self.p.recv_promise( 'C', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 2 )
        self.at(self.p.leader)
        self.at(self.leader_acquired)
        self.an()


    def test_recv_promise_acquire_leadership_without_proposal(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.recv_promise( 'C', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 2 )
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

    def recover(self):
        prev = self.a
        self.a = self.acceptor_factory(self, 'A', 2)
        self.a.recover(prev.promised_id, prev.accepted_id, prev.accepted_value)

    def test_recv_prepare_nack(self):
        self.a.recv_prepare( 'A', PID(2,'A') )
        self.am('promise', 'A', PID(2,'A'), None, None)
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('prepare_nack', 'A', PID(1,'A'), PID(2,'A'))


    def test_recv_prepare_nack_not_active(self):
        self.a.recv_prepare( 'A', PID(2,'A') )
        self.am('promise', 'A', PID(2,'A'), None, None)
        self.a.active = False
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.an()


    def test_recv_accept_request_less_than_promised(self):
        super(PracticalAcceptorTests, self).test_recv_accept_request_less_than_promised()
        self.am('accept_nack', 'A', PID(1,'A'), PID(5,'A'))


    def test_recv_accept_request_less_than_promised(self):
        self.a.recv_prepare( 'A', PID(5,'A') )
        self.am('promise', 'A', PID(5,'A'), None, None)
        self.a.active = False
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.ae( self.a.accepted_value, None )
        self.ae( self.a.accepted_id,    None )
        self.ae( self.a.promised_id,    PID(5,'A'))
        self.an()


    def test_recv_prepare_initial_not_active(self):
        self.ae( self.a.promised_id    , None)
        self.ae( self.a.accepted_value , None)
        self.ae( self.a.accepted_id    , None)
        self.a.active = False
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.an()

        
    def test_recv_prepare_duplicate_not_active(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.active = False
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.an()


    def test_recv_accept_request_duplicate(self):
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accepted', PID(1,'A'), 'foo')
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accepted', PID(1,'A'), 'foo')

        
    def test_recv_accept_request_duplicate_not_active(self):
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accepted', PID(1,'A'), 'foo')
        self.a.active = False
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.an()


    def test_recv_accept_request_initial_not_active(self):
        self.a.active = False
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.an()

        
    def test_recv_accept_request_promised_not_active(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.active = False
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.an()


    # --- Durability Tests ---

    def test_durable_recv_prepare_duplicate(self):
        self.a.recv_prepare( 'A', PID(2,'A') )
        self.am('promise', 'A', PID(2,'A'), None, None)
        self.recover()
        self.a.recv_prepare( 'A', PID(2,'A') )
        self.am('promise', 'A', PID(2,'A'), None, None)

        
    def test_durable_recv_prepare_override(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.clear_msgs()
        self.a.recv_prepare( 'B', PID(2,'B') )
        self.am('promise', 'B', PID(2,'B'), PID(1,'A'), 'foo')


    def test_durable_ignore_prepare_override_until_persisted(self):
        self.a.auto_save = False
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.an()
        self.a.recv_prepare( 'B', PID(2,'B') )
        self.an()
        self.a.persisted()
        self.am('promise', 'A', PID(1,'A'), None, None)


    def test_durable_recv_accept_request_promised(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.recover()
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accepted', PID(1,'A'), 'foo')

        
    def test_durable_recv_accept_request_greater_than_promised(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.recover()
        self.a.recv_accept_request('A', PID(5,'A'), 'foo')
        self.am('accepted', PID(5,'A'), 'foo')


    def test_durable_ignore_new_accept_request_until_persisted(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.auto_save = False
        self.a.recv_accept_request('A', PID(5,'A'), 'foo')
        self.an()
        self.a.recv_accept_request('A', PID(6,'A'), 'foo')
        self.an()
        self.a.persisted()
        self.am('accepted', PID(5,'A'), 'foo')


    def test_durable_recv_accept_request_less_than_promised(self):
        self.a.recv_prepare( 'A', PID(5,'A') )
        self.am('promise', 'A', PID(5,'A'), None, None)
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accept_nack', 'A', PID(1,'A'), PID(5,'A'))
        


class PracticalLearnerTests (test_essential.EssentialLearnerTests):

    def test_basic_resolution_final_acceptors(self):
        self.ae( self.l.final_acceptors, None )
        self.l.recv_accepted( 'A', PID(1,'A'), 'foo' )
        self.l.recv_accepted( 'B', PID(1,'A'), 'foo' )
        self.ae( self.l.final_acceptors, set(['A','B']) )

    def test_add_to_final_acceptors_after_resolution(self):
        self.test_basic_resolution_final_acceptors()
        self.l.recv_accepted( 'C', PID(1,'A'), 'foo' )
        self.ae( self.l.final_acceptors, set(['A','B', 'C']) )

    def test_add_to_final_acceptors_after_resolution_with_pid_mismatch(self):
        self.test_basic_resolution_final_acceptors()
        self.l.recv_accepted( 'C', PID(0,'A'), 'foo' )
        self.ae( self.l.final_acceptors, set(['A','B', 'C']) )
        


    
class NodeTester(PracticalMessenger, unittest.TestCase):

    def test_change_quorum_size(self):
        n = practical.Node(self, 'A', 2)
        self.ae(n.quorum_size, 2)
        n.change_quorum_size(3)
        self.ae(n.quorum_size, 3)
        


class AutoSaveMixin(object):

    auto_save = True
    
    def recv_prepare(self, from_uid, proposal_id):
        super(AutoSaveMixin, self).recv_prepare(from_uid, proposal_id)
        if self.persistance_required and self.auto_save:
            self.persisted()

    def recv_accept_request(self, from_uid, proposal_id, value):
        super(AutoSaveMixin, self).recv_accept_request(from_uid, proposal_id, value)
        if self.persistance_required and self.auto_save:
            self.persisted()


    
class TNode(AutoSaveMixin, practical.Node):
    pass



class PracticalProposerTester(PracticalProposerTests, PracticalMessenger, unittest.TestCase):
    proposer_factory = TNode

class PracticalAcceptorTester(PracticalAcceptorTests, PracticalMessenger, unittest.TestCase):
    acceptor_factory = TNode

class PracticalLearnerTester(PracticalLearnerTests, PracticalMessenger, unittest.TestCase):
    learner_factory = TNode

if __name__ == '__main__':
    unittest.main()

