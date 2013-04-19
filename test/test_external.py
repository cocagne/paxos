import sys
import os.path
import heapq

import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import external

import test_practical
from test_practical import PID


class ExternalMessenger (test_practical.PracticalMessenger):

    procs   = 0
    tleader = None

    
    def send_leadership_proclamation(self, pnum):
        self.procs += 1

        
    def on_leadership_acquired(self):
        super(ExternalMessenger,self).on_leadership_acquired()
        self.tleader = 'gained'

        
    def on_leadership_lost(self):
        self.tleader = 'lost'

        
    def on_leadership_change(self, old_uid, new_uid):
        pass
        


        
        
class ExternalTests (object):

    node_factory = None
    
    def create_node(self):
        self.l = self.node_factory(self, 'A', 3)

        

    def test_initial_leader(self):
        self.l = self.node_factory(self, 'A', 3, 'A')
        self.assertTrue(self.l.leader)
        self.assertEquals( self.procs, 0 )

        
    def test_gain_leader(self):
        self.l.set_proposal('foo')
        self.l.prepare()
        
        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)
        self.l.recv_promise('B', PID(1,'A'), None, None)
        
        self.an()
        
        self.l.recv_promise('C', PID(1,'A'), None, None)
        
        self.am('accept', PID(1,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )
        self.assertEquals( self.l.leader_uid, 'A' )



    def test_gain_leader_nack(self):
        self.l.set_proposal('foo')
        
        self.l.prepare()

        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)

        self.l.recv_prepare_nack('B', PID(1,'A'), PID(2,'C'))

        self.l.prepare()

        self.am('prepare', PID(3,'A'))
        


    def test_lose_leader(self):
        self.test_gain_leader()

        self.assertEquals( self.l.leader_proposal_id, PID(1,'A') )
        
        self.l.recv_leadership_proclamation( 'B', PID(5,'B') )

        self.assertEquals( self.l.leader_proposal_id, PID(5,'B') )
        self.assertEquals( self.tleader, 'lost' )


    def test_lose_leader_via_nacks(self):
        self.test_gain_leader()

        self.assertEquals( self.l.leader_proposal_id, PID(1,'A') )
        
        self.l.recv_accept_nack( 'B', PID(1,'A'), PID(2,'B') )
        self.l.recv_accept_nack( 'C', PID(1,'A'), PID(2,'B') )

        self.assertEquals( self.tleader, 'gained' )
        self.assertEquals( self.l.leader_proposal_id, PID(1,'A') )

        self.l.recv_accept_nack( 'D', PID(1,'A'), PID(2,'B') )

        self.assertEquals( self.l.leader_proposal_id, None )
        self.assertEquals( self.tleader, 'lost' )


    def test_regain_leader(self):
        self.test_lose_leader()
                
        self.l.prepare()

        self.am('prepare', PID(6,'A'))

        self.l.recv_promise('B', PID(6,'A'), None, None)
        self.l.recv_promise('C', PID(6,'A'), None, None)
        self.an()
        self.l.recv_promise('D', PID(6,'A'), None, None)
        self.am('accept', PID(6,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )
        
        

    def test_ignore_old_leader_proclamation(self):
        self.test_lose_leader()

        self.l.recv_leadership_proclamation( 'A', PID(1,'A') )

        self.assertEquals( self.l.leader_proposal_id, PID(5,'B') )


    def test_proposal_id_increment(self):
        self.l.set_proposal('foo')

        self.l.prepare()

        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)
        self.l.recv_promise('B', PID(1,'A'), None, None)
        self.an()
        
        self.l.prepare()
        self.am('prepare', PID(2,'A'))

        self.l.recv_promise('A', PID(2,'A'), None, None)
        self.l.recv_promise('B', PID(2,'A'), None, None)
        self.l.recv_promise('C', PID(2,'A'), None, None)
        
        self.am('accept', PID(2,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )


class ExternalTester(ExternalTests, ExternalMessenger, unittest.TestCase):
    node_factory = external.ExternalNode

    def setUp(self):
        super(ExternalTester,self).setUp()
        self.create_node()
        


class TNode(test_practical.AutoSaveMixin, external.ExternalNode):
    pass

class ExternalProposerTester(test_practical.PracticalProposerTests,
                              ExternalMessenger,
                              unittest.TestCase):
    proposer_factory = TNode

    def setUp(self):
        super(ExternalProposerTester,self).setUp()


class ExternalAcceptorTester(test_practical.PracticalAcceptorTests,
                              ExternalMessenger,
                              unittest.TestCase):
    acceptor_factory = TNode

    def setUp(self):
        super(ExternalAcceptorTester,self).setUp()


class ExternalLearnerTester(test_practical.PracticalLearnerTests,
                              ExternalMessenger,
                              unittest.TestCase):
    learner_factory = TNode

    def setUp(self):
        super(ExternalLearnerTester,self).setUp()


if __name__ == '__main__':
    unittest.main()

