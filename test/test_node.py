import sys
import itertools
import os.path
import pickle

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import node

class Messenger (object):
    leader_acquired = False
    resolution      = None

    def setUp(self):
        self._msgs = list()

    def _append(self, *args):
        self._msgs.append(args)
        
    def send_prepare(self, proposal_id):
        self._append('prepare', proposal_id)

    def send_promise(self, to_uid, proposal_id, proposal_value, accepted_value):
        self._append('promise', to_uid, proposal_id, proposal_value, accepted_value)
        
    def send_prepare_nack(self, to_uid, proposal_id, promised_id):
        self._append('prepare_nack', to_uid, proposal_id, promised_id)

    def send_accept(self, proposal_id, proposal_value):
        self._append('accept', proposal_id, proposal_value)

    def send_accept_nack(self, to_uid, proposal_id, promised_id):
        self._append('accept_nack', to_uid, proposal_id, promised_id)

    def send_accepted(self, to_uid, proposal_id, accepted_value):
        self._append('accepted', to_uid, proposal_id, accepted_value)

    def on_leadership_acquired(self):
        self.leader_acquired = True

    def on_resolution(self, proposal_id, value):
        self.resolution = (proposal_id, value)

    @property
    def last_msg(self):
        return self._msgs[-1]

    def nmsgs(self):
        return len(self._msgs)

    def clear_msgs(self):
        self._msgs = list()

    def am(self, *args):
        self.assertTrue(len(self._msgs) == 1)
        self.assertEquals( self.last_msg, args )
        self._msgs = list()

    def amm(self, msgs):
        self.assertEquals( len(self._msgs), len(msgs) )
        for a, e in itertools.izip(self._msgs, msgs):
            self.assertEquals( a, e )
        self._msgs = list()

    def an(self):
        self.assertTrue( len(self._msgs) == 0 )

    def ae(self, *args):
        self.assertEquals(*args)

    def at(self, *args):
        self.assertTrue(*args)



class ProposerTester (Messenger):

    node_factory = None 

    def setUp(self):
        super(ProposerTester, self).setUp()
        
        self.p = self.node_factory(self, 'A', 2)


    def set_leader(self):
        self.p.prepare()
        self.am('prepare', (1, 'A'))
        self.p.leader = True
        self.ae( self.p.proposed_value, None )


    def test_set_proposal_no_value_not_leader(self):
        self.ae( self.p.proposed_value, None )
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.an()

        
    def test_set_proposal_no_value_as_leader(self):
        self.set_leader()
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.am('accept', (1,'A'), 'foo')


    def test_set_proposal_with_previous_value(self):
        self.p.proposed_value = 'foo'
        self.p.set_proposal( 'bar' )
        self.ae( self.p.proposed_value, 'foo' )
        self.an()
        

    def test_prepare(self):
        self.at( not self.p.leader      )
        self.ae( self.p.node_uid,   'A' )
        self.ae( self.p.quorum_size, 2  )
        self.p.prepare()
        self.am('prepare', (1, 'A'))
        self.at( not self.p.leader )


    def test_prepare_two(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.p.prepare()
        self.am('prepare', (2,'A'))

        
    def test_prepare_with_promises_rcvd(self):
        self.p.prepare()
        self.am('prepare', (1, 'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise('B', (1,'A'), None, None)
        self.ae( len(self.p.promises_rcvd), 1 )
        self.p.prepare()
        self.am('prepare', (2,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )

        
    def test_prepare_increment_on_foriegn_promise(self):
        self.p.recv_promise('B', (5,'C'), None, None)
        self.p.prepare()
        self.am('prepare', (6,'A'))


    def test_preare_no_increment(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise('B', (1,'A'), None, None)
        self.ae( len(self.p.promises_rcvd), 1 )
        self.p.prepare(False)
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 1 )


    def test_observe_proposal(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.p.observe_proposal( 'B', (5,'B') )
        self.p.prepare()
        self.am('prepare', (6,'A'))


    def test_recv_prepare_nack(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.p.recv_prepare_nack( 'B', (1,'A'), (5,'B') )
        self.p.prepare()
        self.am('prepare', (6,'A'))


    def test_resend_accept(self):
        self.p.resend_accept()
        self.an()
        self.set_leader()
        self.p.resend_accept()
        self.an()
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.am('accept', (1,'A'), 'foo')
        self.p.resend_accept()
        self.am('accept', (1,'A'), 'foo')
        

    def test_recv_promise_ignore_other_nodes(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise( 'B', (1,'B'), None, None )
        self.ae( len(self.p.promises_rcvd), 0 )

        
    def test_recv_promise_ignore_when_leader(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.leader = True
        self.p.recv_promise( 'B', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 0 )

        
    def test_recv_promise_ignore_duplicate_response(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise( 'B', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 1 )
        self.p.recv_promise( 'B', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 1 )


    def test_recv_promise_set_proposal_from_null(self):
        self.p.observe_proposal('B', (1,'B'))
        self.p.prepare()
        self.am('prepare', (2,'A'))
        self.ae( self.p.last_accepted_id, None )
        self.ae( self.p.proposed_value, None )
        self.p.recv_promise( 'B', (2,'A'), (1,'B'), 'foo' )
        self.ae( self.p.last_accepted_id, (1,'B') )
        self.ae( self.p.proposed_value, 'foo' )

        
    def test_recv_promise_override_previous_proposal_value(self):
        self.p.observe_proposal('B', (3,'B'))
        self.p.prepare()
        self.am('prepare', (4,'A'))
        self.p.last_accepted_id = (1,'B')
        self.p.proposed_value   = 'foo'
        self.p.recv_promise( 'B', (4,'A'), (3,'B'), 'foo' )
        self.ae( self.p.last_accepted_id, (3,'B') )
        self.ae( self.p.proposed_value, 'foo' )


    def test_recv_promise_acquire_leadership_with_proposal(self):
        self.p.set_proposal('foo')
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise( 'B', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.recv_promise( 'C', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 2 )
        self.at(self.p.leader)
        self.at(self.leader_acquired)
        self.am('accept', (1,'A'), 'foo')


    def test_recv_promise_acquire_leadership_without_proposal(self):
        self.p.prepare()
        self.am('prepare', (1,'A'))
        self.ae( len(self.p.promises_rcvd), 0 )
        self.p.recv_promise( 'B', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 1 )
        self.at(not self.p.leader)
        self.at(not self.leader_acquired)
        self.p.recv_promise( 'C', (1,'A'), None, None )
        self.ae( len(self.p.promises_rcvd), 2 )
        self.at(self.p.leader)
        self.at(self.leader_acquired)
        self.an()



class AcceptorTester (Messenger):

    node_factory = None 

    def setUp(self):
        super(AcceptorTester, self).setUp()
        
        self.a = self.node_factory(self, 'A', 2)


    def test_recv_prepare_initial(self):
        self.ae( self.a.promised_id    , None)
        self.ae( self.a.accepted_value , None)
        self.ae( self.a.accepted_id    , None)
        self.ae( self.a.previous_id    , None)
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('promise', 'A', (1,'A'), None, None)

        
    def test_recv_prepare_duplicate(self):
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('promise', 'A', (1,'A'), None, None)
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('promise', 'A', (1,'A'), None, None)

        
    def test_recv_prepare_override(self):
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('promise', 'A', (1,'A'), None, None)
        self.a.recv_accept_request('A', (1,'A'), 'foo')
        self.clear_msgs()
        self.a.recv_prepare( 'B', (2,'B') )
        self.am('promise', 'B', (2,'B'), (1,'A'), 'foo')

        
    def test_recv_prepare_nack(self):
        self.a.recv_prepare( 'A', (2,'A') )
        self.am('promise', 'A', (2,'A'), None, None)
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('prepare_nack', 'A', (1,'A'), (2,'A'))


    def test_recv_accept_request_initial(self):
        self.a.recv_accept_request('A', (1,'A'), 'foo')
        self.am('accepted', 'A', (1,'A'), 'foo')

        
    def test_recv_accept_request_promised(self):
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('promise', 'A', (1,'A'), None, None)
        self.a.recv_accept_request('A', (1,'A'), 'foo')
        self.am('accepted', 'A', (1,'A'), 'foo')

        
    def test_recv_accept_request_greater_than_promised(self):
        self.a.recv_prepare( 'A', (1,'A') )
        self.am('promise', 'A', (1,'A'), None, None)
        self.a.recv_accept_request('A', (5,'A'), 'foo')
        self.am('accepted', 'A', (5,'A'), 'foo')


    def test_recv_accept_request_less_than_promised(self):
        self.a.recv_prepare( 'A', (5,'A') )
        self.am('promise', 'A', (5,'A'), None, None)
        self.a.recv_accept_request('A', (1,'A'), 'foo')
        self.am('accept_nack', 'A', (1,'A'), (5,'A'))



class LearnerTester (Messenger):

    node_factory = None 

    def setUp(self):
        super(LearnerTester, self).setUp()
        
        self.l = self.node_factory(self, 'A', 2)

    def test_basic_resolution(self):
        self.ae( self.l.quorum_size,       2    )
        self.ae( self.l.final_value,       None )
        self.ae( self.l.final_proposal_id, None )

        self.l.recv_accepted( 'A', (1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', (1,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, (1,'A') )


    def test_ignore_after_resolution(self):
        self.l.recv_accepted( 'A', (1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.at( not self.l.complete )
        self.l.recv_accepted( 'B', (1,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, (1,'A') )

        self.l.recv_accepted( 'A', (5,'A'), 'bar' )
        self.l.recv_accepted( 'B', (5,'A'), 'bar' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, (1,'A') )
        


    def test_ignore_duplicate_messages(self):
        self.l.recv_accepted( 'A', (1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'A', (1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', (1,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, (1,'A') )

        
    def test_ignore_old_messages(self):
        self.l.recv_accepted( 'A', (5,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'A', (1,'A'), 'bar' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', (5,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, (5,'A') )


    def test_overwrite_old_messages(self):
        self.l.recv_accepted( 'A', (1,'A'), 'bar' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', (5,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'A', (5,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, (5,'A') )


class NodeTester(Messenger, unittest.TestCase):

    def test_durability(self):
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
        n = node.Node(self, 'A', 2)
        self.ae(n.quorum_size, 2)
        n.change_quorum_size(3)
        self.ae(n.quorum_size, 3)
        


class NodeProposerTest(ProposerTester, unittest.TestCase):
    node_factory = node.Node

class NodeAcceptorTest(AcceptorTester, unittest.TestCase):
    node_factory = node.Node

class NodeLearnerTest(LearnerTester, unittest.TestCase):
    node_factory = node.Node
