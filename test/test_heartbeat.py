import sys
import os.path
import heapq

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import heartbeat
import test_node


class HMessenger (test_node.Messenger):
        
    def msetup(self):
        self.t = 1
        self.q = []
        self.cp     = 0
        self.hb     = 0
        self.pcount = 0
        self.hbcount = 0
        self.ap = 0
        self.acount = 0
        self.avalue = None
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
        self.hb = pnum
        self.hbcount += 1

    def on_leadership_acquired(self):
        super(HMessenger,self).on_leadership_acquired()
        self.tleader = 'gained'

    def on_leadership_lost(self):
        self.tleader = 'lost'

    def on_leadership_change(self, old_uid, new_uid):
        pass
        


class HNode(heartbeat.HeartbeatNode):
    hb_period       = 2
    liveness_window = 6

    def __init__(self, messenger, *args):
        self.timestamp = messenger.timestamp
        super(HNode,self).__init__(messenger, *args)

        
        
class HeartbeatProposerTester (HMessenger, unittest.TestCase):

    
    def setUp(self):
        super(HeartbeatProposerTester,self).setUp()
        self.msetup()
        self.l = HNode(self, 'A', 3)

        
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
        
        self.assertEquals( self.l.proposal_id, (1,'A') )


    def test_initial_leader(self):
        self.l.leader_proposal_id = (1, 'B')

        self.pre_acq()

        # test_initial_wait() shows that the next call to p() will
        # result in the node attempting to assume leadership by
        # generating a new proposal_id. Reception of this heartbeat
        # should reset the liveness timer and suppress the coup attempt.
        self.l.recv_heartbeat('B', (1,'B'))
        
        self.p()
        self.assertEquals( self.l.proposal_id, None )

        
    def test_gain_leader(self):
        self.pre_acq('foo')
        
        self.p()

        self.am('prepare', (1,'A'))

        self.l.recv_promise('A', (1,'A'), None, None)
        self.l.recv_promise('B', (1,'A'), None, None)
        
        self.an()
        
        self.l.recv_promise('C', (1,'A'), None, None)
        
        self.am('accept', (1,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )


    def test_lose_leader(self):
        self.test_gain_leader()

        self.assertEquals( self.l.leader_proposal_id, (1,'A') )
        
        self.l.recv_heartbeat( 'B', (5,'B') )

        self.assertEquals( self.l.leader_proposal_id, (5,'B') )
        self.assertEquals( self.tleader, 'lost' )


    def test_lose_leader_via_nacks(self):
        self.test_gain_leader()

        self.assertEquals( self.l.leader_proposal_id, (1,'A') )
        
        self.l.recv_accept_nack( 'B', (1,'A'), (2,'B') )
        self.l.recv_accept_nack( 'C', (1,'A'), (2,'B') )

        self.assertEquals( self.tleader, 'gained' )
        self.assertEquals( self.l.leader_proposal_id, (1,'A') )

        self.l.recv_accept_nack( 'D', (1,'A'), (2,'B') )

        self.assertEquals( self.l.leader_proposal_id, None )
        self.assertEquals( self.tleader, 'lost' )


    def test_regain_leader(self):
        self.test_lose_leader()
        
        for i in range(1, 7):
            self.p()
            self.an()

        self.p()

        # The 6 here comes from using (5,'B') as the heartbeat proposal
        # id the caused the loss of leadership
        #
        self.am('prepare', (6,'A'))

        self.l.recv_promise('B', (6,'A'), None, None)
        self.l.recv_promise('C', (6,'A'), None, None)
        self.an()
        self.l.recv_promise('D', (6,'A'), None, None)
        self.am('accept', (6,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )
        
        

    def test_ignore_old_leader_heartbeat(self):
        self.test_lose_leader()

        self.l.recv_heartbeat( 'A', (1,'A') )

        self.assertEquals( self.l.leader_proposal_id, (5,'B') )


    def test_pulse(self):
        self.test_gain_leader()

        for i in range(0,8):
            self.tadvance()

        self.assertEquals( self.hbcount, 5 )
        self.assertEquals( self.l.leader_proposal_id, (1,'A') )
        self.assertTrue( self.l.leader_is_alive() )


    def test_proposal_id_increment(self):
        self.pre_acq('foo')

        self.p()

        self.am('prepare', (1,'A'))

        self.l.recv_promise('A', (1,'A'), None, None)
        self.l.recv_promise('B', (1,'A'), None, None)
        self.an()
        
        self.p()
        self.am('prepare', (2,'A'))

        self.l.recv_promise('A', (2,'A'), None, None)
        self.l.recv_promise('B', (2,'A'), None, None)
        self.l.recv_promise('C', (2,'A'), None, None)
        
        self.am('accept', (2,'A'), 'foo')

        self.assertEquals( self.tleader, 'gained' )
        
