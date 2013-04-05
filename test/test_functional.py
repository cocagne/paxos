import sys
import os.path
import heapq

import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import functional

import test_practical
from test_practical import PID


class HeartbeatMessenger (test_practical.PracticalMessenger):
        
    def msetup(self):
        self.t       = 1
        self.q       = []
        self.hb      = 0
        self.hbcount = 0
        self.tleader = None


    def tadvance(self, incr=1):
        self.t += incr

        while self.q and self.q[0][0] <= self.t:
            heapq.heappop(self.q)[1]()

            
    def timestamp(self):
        return self.t

    
    def schedule(self, when, func_obj):
        whence = when + self.timestamp()
        heapq.heappush(self.q, (when + self.timestamp(), func_obj))

        
    def send_heartbeat(self, pnum):
        self.hb       = pnum
        self.hbcount += 1

        
    def on_leadership_acquired(self):
        super(HeartbeatMessenger,self).on_leadership_acquired()
        self.tleader = 'gained'

        
    def on_leadership_lost(self):
        self.tleader = 'lost'

        
    def on_leadership_change(self, old_uid, new_uid):
        pass
        


class HNode(functional.HeartbeatNode):
    hb_period       = 2
    liveness_window = 6

    def timestamp(self):
        return self.messenger.timestamp()

        
        
class HeartbeatTests (object):

    node_factory = None
    
    def create_node(self):
        self.l = self.node_factory(self, 'A', 3)

        
    def p(self):
        self.tadvance(1)
        self.l.poll_liveness()

        
    def pre_acq(self, value=None):
        if value:
            self.l.set_proposal(value)
            
        for i in range(1,10):
            self.p()
            self.assertEquals( self.l.proposal_id, None )

        self.an()

        
    def test_initial_wait(self):
        self.pre_acq()
            
        self.p()
        
        self.assertEquals( self.l.proposal_id, PID(1,'A') )



    def test_initial_leader(self):
        self.l.leader_proposal_id = PID(1, 'B')

        self.pre_acq()

        # test_initial_wait() shows that the next call to p() will
        # result in the node attempting to assume leadership by
        # generating a new proposal_id. Reception of this heartbeat
        # should reset the liveness timer and suppress the coup attempt.
        self.l.recv_heartbeat('B', PID(1,'B'))
        
        self.p()
        self.assertEquals( self.l.proposal_id, None )

        
    def test_gain_leader(self):
        self.pre_acq('foo')
        
        self.p()

        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)
        self.l.recv_promise('B', PID(1,'A'), None, None)
        
        self.an()
        
        self.l.recv_promise('C', PID(1,'A'), None, None)
        
        self.am('accept', PID(1,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )


    def test_gain_leader_abort(self):
        self.pre_acq('foo')
        
        self.p()

        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)
        self.l.recv_promise('B', PID(1,'A'), None, None)

        self.at( self.l._acquiring )
        self.l.recv_heartbeat( 'B', PID(5,'B') )
        self.at( not self.l._acquiring )
        self.l.acquire_leadership()
        self.an()


    def test_gain_leader_nack(self):
        self.pre_acq('foo')
        
        self.p()

        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)

        self.l.recv_prepare_nack('B', PID(1,'A'), PID(2,'C'))

        self.am('prepare', PID(3,'A'))
        


    def test_lose_leader(self):
        self.test_gain_leader()

        self.assertEquals( self.l.leader_proposal_id, PID(1,'A') )
        
        self.l.recv_heartbeat( 'B', PID(5,'B') )

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
        
        for i in range(1, 7):
            self.p()
            self.an()

        self.p()

        # The 6 here comes from using PID(5,'B') as the heartbeat proposal
        # id the caused the loss of leadership
        #
        self.am('prepare', PID(6,'A'))

        self.l.recv_promise('B', PID(6,'A'), None, None)
        self.l.recv_promise('C', PID(6,'A'), None, None)
        self.an()
        self.l.recv_promise('D', PID(6,'A'), None, None)
        self.am('accept', PID(6,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )
        
        

    def test_ignore_old_leader_heartbeat(self):
        self.test_lose_leader()

        self.l.recv_heartbeat( 'A', PID(1,'A') )

        self.assertEquals( self.l.leader_proposal_id, PID(5,'B') )


    def test_pulse(self):
        self.test_gain_leader()

        for i in range(0,8):
            self.tadvance()

        self.assertEquals( self.hbcount, 5 )
        self.assertEquals( self.l.leader_proposal_id, PID(1,'A') )
        self.assertTrue( self.l.leader_is_alive() )


    def test_proposal_id_increment(self):
        self.pre_acq('foo')

        self.p()

        self.am('prepare', PID(1,'A'))

        self.l.recv_promise('A', PID(1,'A'), None, None)
        self.l.recv_promise('B', PID(1,'A'), None, None)
        self.an()
        
        self.p()
        self.am('prepare', PID(2,'A'))

        self.l.recv_promise('A', PID(2,'A'), None, None)
        self.l.recv_promise('B', PID(2,'A'), None, None)
        self.l.recv_promise('C', PID(2,'A'), None, None)
        
        self.am('accept', PID(2,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )


class HeartbeatTester(HeartbeatTests, HeartbeatMessenger, unittest.TestCase):
    node_factory = HNode

    def setUp(self):
        super(HeartbeatTester,self).setUp()
        self.msetup()
        self.create_node()
        


class TNode(test_practical.AutoSaveMixin, functional.HeartbeatNode):
    pass

class HeartbeatProposerTester(test_practical.PracticalProposerTests,
                              HeartbeatMessenger,
                              unittest.TestCase):
    proposer_factory = TNode

    def setUp(self):
        super(HeartbeatProposerTester,self).setUp()
        self.msetup()

class HeartbeatAcceptorTester(test_practical.PracticalAcceptorTests,
                              HeartbeatMessenger,
                              unittest.TestCase):
    acceptor_factory = TNode

    def setUp(self):
        super(HeartbeatAcceptorTester,self).setUp()
        self.msetup()

class HeartbeatLearnerTester(test_practical.PracticalLearnerTests,
                              HeartbeatMessenger,
                              unittest.TestCase):
    learner_factory = TNode

    def setUp(self):
        super(HeartbeatLearnerTester,self).setUp()
        self.msetup()

if __name__ == '__main__':
    unittest.main()

