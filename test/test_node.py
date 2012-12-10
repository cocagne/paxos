import sys
import os.path

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import node


class TMessenger (object):
    prepare      = None
    promise      = None
    prepare_nack = None
    accept       = None
    accept_nack  = None
    accepted     = None
    lacq         = None
    resolution   = None
    
    def send_prepare(self, proposer_obj, proposal_id):
        self.prepare = proposal_id

    def send_promise(self, proposer_obj, proposal_id, proposal_value, accepted_value):
        self.promise = (proposal_id, proposal_value, accepted_value)
        
    def send_prepare_nack(self, propser_obj, proposal_id):
        self.prepare_nack = proposal_id

    def send_accept(self, proposer_obj, proposal_id, proposal_value):
        self.accept = (proposal_id, proposal_value)

    def send_accept_nack(self, proposer_obj, proposal_id):
        self.accept_nack = proposal_id

    def send_accepted(self, proposer_obj, proposal_id, accepted_value):
        self.accepted = (proposal_id, accepted_value)

    def on_leadership_acquired(self, proposer_obj):
        self.lacq = True

    def on_resolution(self, proposer_obj, proposal_id, value):
        self.resolution = (proposal_id, value)


class MTester (object):

    def setUp(self):
        self.messenger = TMessenger()
        
    def ae(self, attr_name, expected):
        self.assertEquals( getattr(self.messenger, attr_name), expected )
        setattr(self.messenger, attr_name, None)


class ProposerTester (MTester):

    Klass = None

    def setUp(self):
        super(ProposerTester, self).setUp()
        
        self.p = self.Klass(self.messenger, 'uid', 3, 'foo')


    def test_prepare(self):
        self.p.prepare()
        self.ae('prepare', (1,'uid'))
        self.assertTrue( not self.p.leader )


    def test_prepare_two(self):
        self.p.prepare()
        self.p.prepare()
        self.ae('prepare', (2,'uid'))


    def test_prepare_external(self):
        self.p.recv_promise( 'a', (5,'ext'), None, None )
        self.p.prepare()
        self.ae('prepare', (6,'uid'))

    def test_promise_empty(self):
        self.p.prepare()
        self.assertTrue( not self.p.leader )
        self.p.recv_promise( 'a', (1,'uid'), None, None )
        self.assertTrue( not self.p.leader )
        self.ae('accept', None)
        self.p.recv_promise( 'b', (1,'uid'), None, None )
        self.assertTrue( not self.p.leader )
        self.ae('lacq', None)
        self.ae('accept', None)
        self.p.recv_promise( 'c', (1,'uid'), None, None )
        self.ae('accept', ((1,'uid'), 'foo'))
        self.assertTrue( self.p.leader )
        self.ae('lacq', True)

    def test_promise_ignore(self):
        self.p.prepare()
        self.p.recv_promise( 'a', (1,'uid'), None, None )
        self.ae('accept', None)
        self.p.recv_promise( 'b', (1,'uid'), None, None )
        self.ae('accept', None)
        self.p.recv_promise( 'c', (2,'uid'), None, None )
        self.ae('accept', None)

    def test_promise_single(self):
        self.p.prepare()
        self.p.prepare()
        self.p.recv_promise( 'a', (2,'uid'), None, None )
        self.ae('accept', None)
        self.p.recv_promise( 'b', (2,'uid'), 1,    'bar')
        self.ae('accept', None)
        self.p.recv_promise( 'c', (2,'uid'), None, None )
        self.ae('accept', ((2,'uid'), 'bar'))

    def test_promise_multi(self):
        self.p.recv_promise( 'a', (5,'other'), None, None )
        self.p.prepare()
        self.p.recv_promise( 'a', (6,'uid'), 1, 'abc' )
        self.ae('accept', None )
        self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' )
        self.ae('accept', None )
        self.p.recv_promise( 'c', (6,'uid'), 2, 'def' )
        self.ae('accept', ((6,'uid'), 'bar') )

    def test_promise_duplicate(self):
        self.p.recv_promise( 'a', (5,'other'), None, None )
        self.p.prepare()
        self.p.recv_promise( 'a', (6,'uid'), 1, 'abc' )
        self.ae('accept', None )
        self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' )
        self.ae('accept', None )
        self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' )
        self.ae('accept', None )
        self.p.recv_promise( 'c', (6,'uid'), 2, 'def' )
        self.ae('accept', ((6,'uid'), 'bar') )

    def test_promise_old(self):
        self.p.recv_promise( 'a', (5,'other'), None, None )
        self.p.prepare()
        self.p.recv_promise( 'a', (6,'uid'), 1, 'abc' )
        self.ae('accept', None )
        self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' )
        self.ae('accept', None )
        self.p.recv_promise( 'c', (5,'other'), 4, 'baz' )
        self.ae('accept', None )
        self.p.recv_promise( 'd', (6,'uid'), 2, 'def' )
        self.ae('accept', ((6,'uid'), 'bar') )



class AcceptorTester (MTester):

    Klass = None

    def setUp(self):
        super(AcceptorTester, self).setUp()
        self.a = self.Klass(self.messenger, 'uid', 3)

    def p(self, x):
        self.ae('promise', x)

    def r(self, x):
        self.ae('accepted', x)

    def test_first(self):
        self.a.recv_prepare(1)
        self.p( (1, None,None) )
        

    def test_no_value_two(self):
        self.a.recv_prepare(1)
        self.a.recv_prepare(2)
        self.p( (2, 1, None) )

    def test_no_value_ignore_old(self):
        self.a.recv_prepare(2)
        self.p( (2, None, None) )
        self.a.recv_prepare(1)
        self.p( None )

    def test_value_two(self):
        self.a.recv_prepare(1)
        self.p( (1, None, None) )
        self.a.recv_accept_request(1, 'foo')
        self.a.recv_prepare(2)
        self.p( (2, 1, 'foo') )
        self.r( (1, 'foo') )

    def test_value_ignore_old(self):
        self.a.recv_prepare(2)
        self.p( (2, None, None) )
        self.a.recv_accept_request(2, 'foo')
        self.r( (2, 'foo') )
        self.a.recv_prepare(1)
        self.p( None )
        self.r( None )

    def test_prepared_accept(self):
        self.a.recv_prepare(1)
        self.a.recv_accept_request(1, 'foo')
        self.r( (1,'foo'))

    def test_unprepared_accept(self):
        self.a.recv_accept_request(1, 'foo')
        self.r( (1,'foo'))

    def test_ignored_accept(self):
        self.a.recv_prepare(5)
        self.a.recv_accept_request(1, 'foo')
        self.r(None)

    def test_duplicate_accept(self):
        self.a.recv_accept_request(1, 'foo')
        self.r((1,'foo'))
        self.a.recv_accept_request(1, 'foo')
        self.r((1,'foo'))

    def test_ignore_after_accept(self):
        self.a.recv_accept_request(5, 'foo')
        self.r( (5, 'foo') )
        self.a.recv_prepare(1)
        self.r( None )

    
class LearnerTester (MTester):

    Klass = None

    def setUp(self):
        super(LearnerTester, self).setUp()
        self.l = self.Klass(self.messenger, 'uid', 3)

    def v(self, ignore, x):
        self.assertEquals( self.l.final_value, x )

    def test_one(self):
        self.v( self.l.recv_accepted(1, (1,'1'), 'foo'), None )

    def test_two(self):
        self.v( self.l.recv_accepted(1, (1,'1'), 'foo'), None )
        self.v( self.l.recv_accepted(2, (1,'1'), 'foo'), None )

    def test_three(self):
        self.v( self.l.recv_accepted(1, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(3, (1,'1'), 'foo'), 'foo' )

    def test_three(self):
        self.v( self.l.recv_accepted(1, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(3, (1,'1'), 'foo'), 'foo' )

    def test_duplicates(self):
        self.v( self.l.recv_accepted(1, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.v( self.l.recv_accepted(3, (1,'1'), 'foo'), 'foo' )

    def test_ignore_one(self):
        self.v( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
        self.v( self.l.recv_accepted(3, (1,'1'), 'bar'), None  )
        self.v( self.l.recv_accepted(4, (2,'2'), 'foo'), 'foo' )

    def test_ignore_old(self):
        self.v( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (1,'1'), 'bar'), None  )
        self.v( self.l.recv_accepted(4, (2,'2'), 'foo'), 'foo' )

    def test_override_old(self):
        self.v( self.l.recv_accepted(1, (1,'1'), 'bar'), None  )
        self.v( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
        self.v( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
        self.v( self.l.recv_accepted(3, (2,'2'), 'foo'), 'foo' )

    #def test_ignore_done(self):
    #    self.assertEquals( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
    #    self.assertEquals( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
    #    self.assertEquals( self.l.recv_accepted(3, (2,'2'), 'foo'), 'foo' )
    #    self.assertEquals( self.l.recv_accepted(1, (3,'3'), 'foo'), 'foo' )
    


        
class NodeProposerTest(ProposerTester, unittest.TestCase):
    Klass = node.Node

class NodeAcceptorTest(AcceptorTester, unittest.TestCase):
    Klass = node.Node

class NodeLearnerTest(LearnerTester, unittest.TestCase):
    Klass = node.Node
