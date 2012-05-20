'''
The intent of this module is to provide a minimally correct implementation of
the Paxos algorithm. These classes may be used as-is to provide correctness to
more advanced implementations that enhance the basic model with such things as
timeouts, retransmit, liveness-detectors, etc. 

Instances of these classes are intended for a single instance of the algorithm
only.

As this is an algorithm-only implementation that has no notion of "message
passing", the return values of the function are used in place of message
passing. Whenever a function returns None, it indicates that no messages
should be sent. Otherwise, the next message in the Paxos sequence must be
sent with the contents of the return value.
'''

class Proposer (object):

    def __init__(self, proposer_uid, quorum_size, proposed_value=None):
        self.proposer_uid         = proposer_uid
        self.proposal_id          = None
        self.next_proposal_number = 1
        self.accepted_id          = None
        self.replied              = set()
        self.value                = proposed_value
        self.quorum_size          = quorum_size

        self.leader               = False


    def set_proposal(self, value):
        '''
        Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted. 
        '''
        if self.value is None:
            self.value = value

            
    def prepare(self):
        '''
        Returns a new proposal id that is higher than any previously seen proposal id.
        The proposal id is a tuple of (proposal_numer, proposer_uid)
        '''
        self.leader  = False
        self.replied = set()

        self.proposal_id = (self.next_proposal_number, self.proposer_uid)
        
        self.next_proposal_number += 1
        
        return self.proposal_id

    

    def observe_proposal(self, proposal_id):
        '''
        Optional method used to update the proposal counter as proposals are seen on the network.
        When co-located with Acceptors and/or Learners, this method may be used to avoid a message
        delay when attempting to assume leadership (guaranteed NACK if the proposal number is too low).
        '''
        if proposal_id >= (self.next_proposal_number, self.proposer_uid):
            self.next_proposal_number = proposal_id[0] + 1



    def recv_promise(self, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):
        '''
        acceptor_uid - Needed to ensure duplicate messages from this node are ignored
        
        Returns: None for no action or (proposal_number, value) for Accept! message
        '''
        if proposal_id >= (self.next_proposal_number, self.proposer_uid):
            self.next_proposal_number = proposal_id[0] + 1

        if self.leader or proposal_id != self.proposal_id or acceptor_uid in self.replied:
            return

        self.replied.add( acceptor_uid )
        
        if prev_proposal_id > self.accepted_id:
            self.accepted_id = prev_proposal_id
            if prev_proposal_value is not None:
                self.value   = prev_proposal_value

        if len(self.replied) == self.quorum_size:
            self.leader = True

            return self.proposal_id, self.value



        
class Acceptor (object):

    
    def __init__(self):
        self.promised_id     = None
        self.accepted_value  = None
        self.accepted_id     = None
        self.previous_id     = None            

        
    def recv_prepare(self, proposal_id):
        '''
        Returns: None on prepare failed. (proposal_id, promised_id, accepted_value) on success
        '''
        if proposal_id == self.promised_id:
            # Duplicate accepted proposal
            return proposal_id, self.previous_id, self.accepted_value
        
        if proposal_id > self.promised_id:
            self.previous_id = self.promised_id
            
            self.promised_id = proposal_id

            return proposal_id, self.previous_id, self.accepted_value

        
    def recv_accept_request(self, proposal_id, value):
        '''
        Returns: None on request denied. (proposal_id, accepted_value) on accepted
        '''
        if proposal_id >= self.promised_id:
            self.accepted_value  = value
            self.promised_id = proposal_id
            return proposal_id, self.accepted_value



        
class Learner (object):
    
    def __init__(self, quorum_size):
        self.proposals   = dict() # maps proposal_id => [accept_count, retain_count, value]
        self.acceptors   = dict() # maps acceptor_uid => last_accepted_proposal_id
        self.quorum_size = quorum_size

        self.accepted_value       = None
        self.accepted_proposal_id = None
        self.complete             = False


    def recv_accepted(self, acceptor_uid, proposal_id, accepted_value):
        '''
        Only messages from valid acceptors may result in calling this function.
        '''
        if self.accepted_value is not None:
            # already done
            return self.accepted_value
        
        last_pn = self.acceptors.get(acceptor_uid)

        if not proposal_id > last_pn:
            return # Old message

        self.acceptors[ acceptor_uid ] = proposal_id
        
        if last_pn is not None:
            oldp = self.proposals[ last_pn ]
            oldp[1] -= 1
            if oldp[1] == 0:
                del self.proposals[ last_pn ]

        if not proposal_id in self.proposals:
            self.proposals[ proposal_id ] = [0, 0, accepted_value]

        t = self.proposals[ proposal_id ]

        assert accepted_value == t[2], 'Value mismatch for single proposal!'
        
        t[0] += 1
        t[1] += 1

        if t[0] == self.quorum_size:
            self.accepted_value       = accepted_value
            self.accepted_proposal_id = proposal_id
            self.proposals            = None
            self.acceptors            = None
            self.complete             = True

        return self.accepted_value
            


    
class Node (object):
    '''
    This class supports the common model where each node on a network preforms
    all three Paxos roles, Proposer, Acceptor, and Learner.
    '''

    def __init__(self, proposer, acceptor, learner, resolution_callback):
        '''
        proposer - An object implementing the Proposer API
        acceptor - An object implementing the Acceptor API
        learner  - An object implementing the Learner API
        
        resolution_callback - Function that will be called when a resolution for
                              the Paxos instance is determined. The arguments
                              will be: proposal_id, value
        '''
        self.proposer      = proposer
        self.acceptor      = acceptor
        self.learner       = learner
        self.on_resolution = resolution_callback

    # -- Proposer Methods --
        
    prepare          = property( lambda self: self.proposer.prepare )
    observe_proposal = property( lambda self: self.proposer.observe_proposal )
    recv_promise     = property( lambda self: self.proposer.recv_promise )
    set_proposal     = property( lambda self: self.proposer.set_proposal )
    
    # -- Acceptor Methods --
    
    def recv_prepare(self, proposal_id):
        self.proposer.observe_proposal( proposal_id )
        return self.acceptor.recv_prepare( proposal_id )

    recv_accept_request = property( lambda self: self.acceptor.recv_accept_request )
    
    # -- Learner Methods --

    def recv_accepted(self, acceptor_uid, proposal_id, accepted_value):
        r = self.learner.recv_accepted( acceptor_uid, proposal_id, accepted_value )

        if self.learner.complete:
            self.on_resolution( self.learner.accepted_proposal_id, self.learner.accepted_value )

        return r

