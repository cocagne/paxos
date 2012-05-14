import sys
import os.path

from twisted.trial import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import basic

class ProposerTester (object):

    Klass = None

    def setUp(self):
        self.p = self.Klass('uid', 3, 'foo')


    def test_prepare(self):
        self.assertEquals( self.p.prepare(), (1,'uid') )

    def test_prepare_two(self):
        self.p.prepare()
        self.assertEquals( self.p.prepare(), (2,'uid') )

    def test_prepare_external(self):
        self.p.recv_promise( 'a', (5,'ext'), None, None )
        self.assertEquals( self.p.prepare(), (6,'uid') )

    def test_promise_empty(self):
        self.p.prepare()
        self.assertEquals( self.p.recv_promise( 'a', (1,'uid'), None, None ), None )
        self.assertEquals( self.p.recv_promise( 'b', (1,'uid'), None, None ), None )
        self.assertEquals( self.p.recv_promise( 'c', (1,'uid'), None, None ), ((1,'uid'), 'foo') )

    def test_promise_ignore(self):
        self.p.prepare()
        self.assertEquals( self.p.recv_promise( 'a', (1,'uid'), None, None ), None )
        self.assertEquals( self.p.recv_promise( 'b', (1,'uid'), None, None ), None )
        self.assertEquals( self.p.recv_promise( 'c', (2,'uid'), None, None ), None )

    def test_promise_single(self):
        self.p.prepare()
        self.p.prepare()
        self.assertEquals( self.p.recv_promise( 'a', (2,'uid'), None, None ), None )
        self.assertEquals( self.p.recv_promise( 'b', (2,'uid'), 1,    'bar'), None )
        self.assertEquals( self.p.recv_promise( 'c', (2,'uid'), None, None ), ((2,'uid'), 'bar') )

    def test_promise_multi(self):
        self.p.recv_promise( 'a', (5,'other'), None, None )
        self.p.prepare()
        self.assertEquals( self.p.recv_promise( 'a', (6,'uid'), 1, 'abc' ), None )
        self.assertEquals( self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' ), None )
        self.assertEquals( self.p.recv_promise( 'c', (6,'uid'), 2, 'def' ), ((6,'uid'), 'bar') )

    def test_promise_duplicate(self):
        self.p.recv_promise( 'a', (5,'other'), None, None )
        self.p.prepare()
        self.assertEquals( self.p.recv_promise( 'a', (6,'uid'), 1, 'abc' ), None )
        self.assertEquals( self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' ), None )
        self.assertEquals( self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' ), None )
        self.assertEquals( self.p.recv_promise( 'c', (6,'uid'), 2, 'def' ), ((6,'uid'), 'bar') )

    def test_promise_old(self):
        self.p.recv_promise( 'a', (5,'other'), None, None )
        self.p.prepare()
        self.assertEquals( self.p.recv_promise( 'a', (6,'uid'), 1, 'abc' ), None )
        self.assertEquals( self.p.recv_promise( 'b', (6,'uid'), 3, 'bar' ), None )
        self.assertEquals( self.p.recv_promise( 'c', (5,'other'), 4, 'baz' ), None )
        self.assertEquals( self.p.recv_promise( 'd', (6,'uid'), 2, 'def' ), ((6,'uid'), 'bar') )



class AcceptorTester (object):

    Klass = None

    def setUp(self):
        self.a = self.Klass()


    def test_first(self):
        self.assertEquals( self.a.recv_prepare(1), (1, None,None) )

    def test_no_value_two(self):
        self.a.recv_prepare(1)
        self.assertEquals( self.a.recv_prepare(2), (2, 1, None) )

    def test_no_value_ignore_old(self):
        self.a.recv_prepare(2)
        self.assertEquals( self.a.recv_prepare(1), None )

    def test_value_two(self):
        self.a.recv_prepare(1)
        self.a.recv_accept_request(1, 'foo')
        self.assertEquals( self.a.recv_prepare(2), (2, 1, 'foo') )

    def test_value_ignore_old(self):
        self.a.recv_prepare(2)
        self.a.recv_accept_request(2, 'foo')
        self.assertEquals( self.a.recv_prepare(1), None )

    def test_prepared_accept(self):
        self.a.recv_prepare(1)
        self.assertEquals(self.a.recv_accept_request(1, 'foo'), (1,'foo'))

    def test_unprepared_accept(self):
        self.assertEquals(self.a.recv_accept_request(1, 'foo'), (1,'foo'))

    def test_ignored_accept(self):
        self.a.recv_prepare(5)
        self.assertEquals(self.a.recv_accept_request(1, 'foo'), None)

    def test_duplicate_accept(self):
        self.assertEquals(self.a.recv_accept_request(1, 'foo'), (1,'foo'))
        self.assertEquals(self.a.recv_accept_request(1, 'foo'), (1,'foo'))

    def test_ignore_after_accept(self):
        self.a.recv_accept_request(5, 'foo')
        self.assertEquals( self.a.recv_prepare(1), None )

    
class LearnerTester (object):

    Klass = None

    def setUp(self):
        self.l = self.Klass(3)


    def test_one(self):
        self.assertEquals( self.l.recv_accepted(1, (1,'1'), 'foo'), None )

    def test_two(self):
        self.assertEquals( self.l.recv_accepted(1, (1,'1'), 'foo'), None )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'foo'), None )

    def test_three(self):
        self.assertEquals( self.l.recv_accepted(1, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(3, (1,'1'), 'foo'), 'foo' )

    def test_three(self):
        self.assertEquals( self.l.recv_accepted(1, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(3, (1,'1'), 'foo'), 'foo' )

    def test_duplicates(self):
        self.assertEquals( self.l.recv_accepted(1, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(3, (1,'1'), 'foo'), 'foo' )

    def test_ignore_one(self):
        self.assertEquals( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(3, (1,'1'), 'bar'), None  )
        self.assertEquals( self.l.recv_accepted(4, (2,'2'), 'foo'), 'foo' )

    def test_ignore_old(self):
        self.assertEquals( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (1,'1'), 'bar'), None  )
        self.assertEquals( self.l.recv_accepted(4, (2,'2'), 'foo'), 'foo' )

    def test_override_old(self):
        self.assertEquals( self.l.recv_accepted(1, (1,'1'), 'bar'), None  )
        self.assertEquals( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
        self.assertEquals( self.l.recv_accepted(3, (2,'2'), 'foo'), 'foo' )

    #def test_ignore_done(self):
    #    self.assertEquals( self.l.recv_accepted(1, (2,'2'), 'foo'), None  )
    #    self.assertEquals( self.l.recv_accepted(2, (2,'2'), 'foo'), None  )
    #    self.assertEquals( self.l.recv_accepted(3, (2,'2'), 'foo'), 'foo' )
    #    self.assertEquals( self.l.recv_accepted(1, (3,'3'), 'foo'), 'foo' )
    


        
class BasicProposerTest(ProposerTester, unittest.TestCase):
    Klass = basic.Proposer

class BasicAcceptorTest(AcceptorTester, unittest.TestCase):
    Klass = basic.Acceptor

class BasicLearnerTest(LearnerTester, unittest.TestCase):
    Klass = basic.Learner

