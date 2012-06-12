import sys
import os.path
import tempfile
import shutil

#from twisted.trial import unittest
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from paxos import multi

import test_basic

class MultiToBasic ( multi.MultiPaxos ):
    def __init__(self, proposer_uid='uid', quorum_size=3, proposed_value=None):
        super(MultiToBasic, self).__init__()

        self.initialize(proposer_uid, quorum_size, 1)
        
        if proposed_value is not None:
            self.node.set_proposal(proposed_value)

    def set_proposal(self, value):
        return super(MultiToBasic, self).set_proposal(1, value)
            
    def prepare(self):
        return super(MultiToBasic, self).prepare()

    def recv_promise(self, acceptor_uid, proposal_number, prev_proposal_number, prev_proposal_value):
        return super(MultiToBasic, self).recv_promise(1, acceptor_uid, proposal_number, prev_proposal_number, prev_proposal_value)

    def recv_prepare(self, proposal_number):
        return super(MultiToBasic, self).recv_prepare(1, proposal_number)

    def recv_accept_request(self, proposal_number, value):
        return super(MultiToBasic, self).recv_accept_request(1, proposal_number, value)

    def recv_accepted(self, acceptor_uid, proposal_number, accepted_value):
        return super(MultiToBasic, self).recv_accepted(1, acceptor_uid, proposal_number, accepted_value)

    
class MultiProposerTest(test_basic.ProposerTester, unittest.TestCase):
    Klass = MultiToBasic

class MultiAcceptorTest(test_basic.AcceptorTester, unittest.TestCase):
    Klass = MultiToBasic

class MultiLearnerTest(test_basic.LearnerTester, unittest.TestCase):
    Klass = MultiToBasic


class MultiTester (unittest.TestCase):

    def setUp(self):
        self.m = multi.MultiPaxos()
        self.m.initialize(1, 3, 1)

    def test_simple(self):
        m = multi.MultiPaxos()
        m.initialize(1, 3, 1)
        
        pnum = m.prepare()
        pval = 2
        inum = m.instance_num

        m.set_proposal(inum, pval)
        
        m.recv_prepare(inum, pnum)
        
        m.recv_promise(inum, 1, pnum, None, None)
        m.recv_promise(inum, 2, pnum, None, None)
        m.recv_promise(inum, 3, pnum, None, None)

        self.assertTrue( m.node.proposer.leader )

        self.assertEquals(m.recv_accept_request(inum, pnum, pval), (pnum, pval))

        m.recv_accepted(inum, 1, pnum, pval)
        m.recv_accepted(inum, 2, pnum, pval)

        self.assertEquals( m.instance_num, 1 )
        
        m.recv_accepted(inum, 3, pnum, pval)

        self.assertEquals( m.instance_num, 2 )


    def test_ignore_previous_messages(self):
        m    = multi.MultiPaxos()
        m.initialize(1, 3, 2)
        pnum = m.prepare()
        pval = 2
        inum = m.instance_num

        m.set_proposal(inum, pval)
        
        m.recv_prepare(inum, pnum)
        
        m.recv_promise(inum, 1, pnum, None, None)
        m.recv_promise(inum, 2, pnum, None, None)
        
        m.recv_promise(1, 1, pnum, None, None)
        m.recv_promise(1, 2, pnum, None, None)
        m.recv_promise(1, 3, pnum, None, None)

        self.assertTrue( not m.node.proposer.leader )
        
        m.recv_promise(inum, 3, pnum, None, None)

        self.assertTrue( m.node.proposer.leader )

        self.assertEquals(m.recv_accept_request(1, pnum, pval), None)
        self.assertEquals(m.recv_accept_request(inum, pnum, pval), (pnum, pval))

        m.recv_accepted(inum, 1, pnum, pval)
        m.recv_accepted(inum, 2, pnum, pval)

        m.recv_accepted(1, 1, pnum, pval)
        m.recv_accepted(1, 2, pnum, pval)
        m.recv_accepted(1, 3, pnum, pval)

        self.assertEquals( m.instance_num, 2 )
        
        m.recv_accepted(inum, 3, pnum, pval)

        self.assertEquals( m.instance_num, 3 )



class MultiDurabilityTester (unittest.TestCase):

    def setUp(self):
        self.tdir = tempfile.mkdtemp()
        self.m    = multi.MultiPaxos(self.tdir, 'foo')
        self.m.initialize(1, 3, 1)

    def tearDown(self):
        shutil.rmtree(self.tdir)


    def test_durability(self):
        m = multi.MultiPaxos(self.tdir, 'foo')
        
        pnum = m.prepare()
        pval = 2
        inum = m.instance_num

        m.set_proposal(inum, pval)
        
        m.recv_prepare(inum, pnum)

        # crash & recover 
        m = multi.MultiPaxos(self.tdir, 'foo')     

        m.recv_promise(inum, 1, pnum, None, None)
        m.recv_promise(inum, 2, pnum, None, None) 
        m.recv_promise(inum, 3, pnum, None, None)

        # crash & recover 
        m = multi.MultiPaxos(self.tdir, 'foo')

        self.assertTrue( m.node.proposer.leader )

        self.assertEquals(m.recv_accept_request(inum, pnum, pval), (pnum, pval))

        m.recv_accepted(inum, 1, pnum, pval)
        m.recv_accepted(inum, 2, pnum, pval)
        
        self.assertEquals( m.instance_num, 1 )
        
        m.recv_accepted(inum, 3, pnum, pval)

        # crash & recover 
        m = multi.MultiPaxos(self.tdir, 'foo')

        self.assertEquals( m.instance_num, 2 )


