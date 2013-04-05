import sys
import itertools
import os.path
import pickle

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import essential


PID = essential.ProposalID


class EssentialMessenger (object):

    resolution = None

    def setUp(self):
        self._msgs = list()

    def _append(self, *args):
        self._msgs.append(args)
        
    def send_prepare(self, proposal_id):
        self._append('prepare', proposal_id)

    def send_promise(self, to_uid, proposal_id, proposal_value, accepted_value):
        self._append('promise', to_uid, proposal_id, proposal_value, accepted_value)
        
    def send_accept(self, proposal_id, proposal_value):
        self._append('accept', proposal_id, proposal_value)

    def send_accepted(self, proposal_id, accepted_value):
        self._append('accepted', proposal_id, accepted_value)

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





class EssentialProposerTests (object):

    proposer_factory = None 

    def setUp(self):
        super(EssentialProposerTests, self).setUp()
        
        self.p = self.proposer_factory(self, 'A', 2)


    def al(self, value):
        if hasattr(self.p, 'leader'):
            self.assertEquals( self.p.leader, value )

    def num_promises(self):
        if hasattr(self.p, 'promises_rcvd'):
            return len(self.p.promises_rcvd) # python version
        else:
            return self.p.numPromises() # java version

        
    def test_set_proposal_no_value(self):
        self.ae( self.p.proposed_value, None )
        self.p.set_proposal( 'foo' )
        self.ae( self.p.proposed_value, 'foo' )
        self.an()

        
    def test_set_proposal_with_previous_value(self):
        self.p.set_proposal( 'foo' )
        self.p.set_proposal( 'bar' )
        self.ae( self.p.proposed_value, 'foo' )
        self.an()
        

    def test_prepare(self):
        self.al( False )
        self.ae( self.p.proposer_uid, 'A' )
        self.ae( self.p.quorum_size,  2  )
        self.p.prepare()
        self.am('prepare', PID(1, 'A'))
        self.al( False )


    def test_prepare_two(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.p.prepare()
        self.am('prepare', PID(2,'A'))

        
    def test_prepare_with_promises_rcvd(self):
        self.p.prepare()
        self.am('prepare', PID(1, 'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise('B', PID(1,'A'), None, None)
        self.ae( self.num_promises(), 1 )
        self.p.prepare()
        self.am('prepare', PID(2,'A'))
        self.ae( self.num_promises(), 0 )

        
    def test_recv_promise_ignore_other_nodes(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise( 'B', PID(1,'B'), None, None )
        self.ae( self.num_promises(), 0 )
    
        
    def test_recv_promise_ignore_duplicate_response(self):
        self.p.prepare()
        self.am('prepare', PID(1,'A'))
        self.ae( self.num_promises(), 0 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 1 )
        self.p.recv_promise( 'B', PID(1,'A'), None, None )
        self.ae( self.num_promises(), 1 )


    def test_recv_promise_set_proposal_from_null(self):
        self.p.prepare()
        self.clear_msgs()
        self.p.prepare()
        self.am('prepare', PID(2,'A'))
        self.ae( self.p.last_accepted_id, None )
        self.ae( self.p.proposed_value, None )
        self.p.recv_promise( 'B', PID(2,'A'), PID(1,'B'), 'foo' )
        self.ae( self.p.last_accepted_id, PID(1,'B') )
        self.ae( self.p.proposed_value, 'foo' )

        
    def test_recv_promise_override_previous_proposal_value(self):
        self.p.prepare()
        self.p.prepare()
        self.p.prepare()
        self.p.recv_promise( 'B', PID(3,'A'), PID(1,'B'), 'foo' )
        self.clear_msgs()
        self.p.prepare()
        self.am('prepare', PID(4,'A'))
        self.p.recv_promise( 'B', PID(4,'A'), PID(3,'B'), 'bar' )
        self.ae( self.p.last_accepted_id, PID(3,'B') )
        self.ae( self.p.proposed_value, 'bar' )

        
    def test_recv_promise_ignore_previous_proposal_value(self):
        self.p.prepare()
        self.p.prepare()
        self.p.prepare()
        self.p.recv_promise( 'B', PID(3,'A'), PID(1,'B'), 'foo' )
        self.clear_msgs()
        self.p.prepare()
        self.am('prepare', PID(4,'A'))
        self.p.recv_promise( 'B', PID(4,'A'), PID(3,'B'), 'bar' )
        self.ae( self.p.last_accepted_id, PID(3,'B') )
        self.ae( self.p.proposed_value, 'bar' )
        self.p.recv_promise( 'C', PID(4,'A'), PID(2,'B'), 'baz' )
        self.ae( self.p.last_accepted_id, PID(3,'B') )
        self.ae( self.p.proposed_value, 'bar' )



        
class EssentialAcceptorTests (object):

    acceptor_factory = None 

    def setUp(self):
        super(EssentialAcceptorTests, self).setUp()
        
        self.a = self.acceptor_factory(self, 'A', 2)


    def test_recv_prepare_initial(self):
        self.ae( self.a.promised_id    , None)
        self.ae( self.a.accepted_value , None)
        self.ae( self.a.accepted_id    , None)
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)

        
    def test_recv_prepare_duplicate(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)

        
    def test_recv_prepare_override(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.clear_msgs()
        self.a.recv_prepare( 'B', PID(2,'B') )
        self.am('promise', 'B', PID(2,'B'), PID(1,'A'), 'foo')


    def test_recv_accept_request_initial(self):
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accepted', PID(1,'A'), 'foo')

        
    def test_recv_accept_request_promised(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.am('accepted', PID(1,'A'), 'foo')

        
    def test_recv_accept_request_greater_than_promised(self):
        self.a.recv_prepare( 'A', PID(1,'A') )
        self.am('promise', 'A', PID(1,'A'), None, None)
        self.a.recv_accept_request('A', PID(5,'A'), 'foo')
        self.am('accepted', PID(5,'A'), 'foo')


    def test_recv_accept_request_less_than_promised(self):
        self.a.recv_prepare( 'A', PID(5,'A') )
        self.am('promise', 'A', PID(5,'A'), None, None)
        self.a.recv_accept_request('A', PID(1,'A'), 'foo')
        self.ae( self.a.accepted_value, None )
        self.ae( self.a.accepted_id,    None )
        self.ae( self.a.promised_id,    PID(5,'A'))



class EssentialLearnerTests (object):

    learner_factory = None 

    def setUp(self):
        super(EssentialLearnerTests, self).setUp()
        
        self.l = self.learner_factory(self, 'A', 2)

    def test_basic_resolution(self):
        self.ae( self.l.quorum_size,       2    )
        self.ae( self.l.final_value,       None )
        self.ae( self.l.final_proposal_id, None )

        self.l.recv_accepted( 'A', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, PID(1,'A') )


    def test_ignore_after_resolution(self):
        self.l.recv_accepted( 'A', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.at( not self.l.complete )
        self.l.recv_accepted( 'B', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, PID(1,'A') )

        self.l.recv_accepted( 'A', PID(5,'A'), 'bar' )
        self.l.recv_accepted( 'B', PID(5,'A'), 'bar' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, PID(1,'A') )
        


    def test_ignore_duplicate_messages(self):
        self.l.recv_accepted( 'A', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'A', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', PID(1,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, PID(1,'A') )

        
    def test_ignore_old_messages(self):
        self.l.recv_accepted( 'A', PID(5,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'A', PID(1,'A'), 'bar' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', PID(5,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, PID(5,'A') )


    def test_overwrite_old_messages(self):
        self.l.recv_accepted( 'A', PID(1,'A'), 'bar' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'B', PID(5,'A'), 'foo' )
        self.ae( self.l.final_value, None )
        self.l.recv_accepted( 'A', PID(5,'A'), 'foo' )
        self.ae( self.l.final_value, 'foo' )
        self.ae( self.l.final_proposal_id, PID(5,'A') )



class TPax(object):
    def __init__(self, messenger, uid, quorum_size):
        self.messenger     = messenger
        self.proposer_uid  = uid
        self.quorum_size   = quorum_size

        
class TProposer (essential.Proposer, TPax):
    pass

class TAcceptor (essential.Acceptor, TPax):
    pass

class TLearner (essential.Learner, TPax):
    pass


class EssentialProposerTester(EssentialProposerTests, EssentialMessenger, unittest.TestCase):
    proposer_factory = TProposer

class EssentialAcceptorTester(EssentialAcceptorTests, EssentialMessenger, unittest.TestCase):
    acceptor_factory = TAcceptor

class EssentialLearnerTester(EssentialLearnerTests, EssentialMessenger, unittest.TestCase):
    learner_factory = TLearner

if __name__ == '__main__':
    unittest.main()

