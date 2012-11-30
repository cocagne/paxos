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


class Messenger (object):
    def send_prepare(self, proposer_obj, proposal_id):
        '''
        Sends a Prepare message
        '''

    def send_promise(self, proposer_obj, proposal_id, proposal_value, accepted_value):
        '''
        Sends a Promise message
        '''

    def send_prepare_nack(self, propser_obj, proposal_id):
        '''
        Sends a Prepare Nack message for the proposal
        '''

    def send_accept(self, proposer_obj, proposal_id, proposal_value):
        '''
        Sends an Accept! message
        '''

    def send_accept_nack(self, proposer_obj, proposal_id):
        '''
        Sends a Accept! Nack message for the proposal
        '''

    def send_accepted(self, proposer_obj, proposal_id, accepted_value):
        '''
        Sends an Accepted message
        '''

    def on_leadership_acquired(self, proposer_obj):
        '''
        Called when leadership has been aquired
        '''

    def on_leadership_lost(self, proposer_obj):
        '''
        Called when leadership has been lost
        '''

    def on_resolution(self, proposer_obj, proposal_id, value):
        '''
        Called when a resolution is reached
        '''



class Proposer (object):

    messenger            = None
    node_uid             = None
    quorum_size          = None

    proposed_value       = None
    proposal_id          = None
    last_accepted_id     = None
    next_proposal_number = 1
    promises_rcvd        = None
    leader               = False


    def set_proposal(self, value):
        '''
        Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted. 
        '''
        if self.proposed_value is None:
            self.proposed_value = value

            
    def prepare(self):
        '''
        Returns a new proposal id that is higher than any previously seen proposal id.
        The proposal id is a tuple of (proposal_numer, node_uid)
        '''
        self.leader        = False
        self.promises_rcvd = set()

        self.proposal_id = (self.next_proposal_number, self.node_uid)
        
        self.next_proposal_number += 1

        self.messenger.send_prepare(self, self.proposal_id)
        
        return self.proposal_id

    

    def observe_proposal(self, proposal_id):
        '''
        Optional method used to update the proposal counter as proposals are seen on the network.
        When co-located with Acceptors and/or Learners, this method may be used to avoid a message
        delay when attempting to assume leadership (guaranteed NACK if the proposal number is too low).
        '''
        if proposal_id >= (self.next_proposal_number, self.node_uid):
            self.next_proposal_number = proposal_id[0] + 1



    def recv_promise(self, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):
        '''
        acceptor_uid - Needed to ensure duplicate messages from this node are ignored
        
        Returns: None for no action or (proposal_number, value) for Accept! message
        '''
        if proposal_id >= (self.next_proposal_number, self.node_uid):
            self.next_proposal_number = proposal_id[0] + 1

        if self.leader or proposal_id != self.proposal_id or acceptor_uid in self.promises_rcvd:
            return

        self.promises_rcvd.add( acceptor_uid )
        
        if prev_proposal_id > self.last_accepted_id:
            self.last_accepted_id = prev_proposal_id
            if prev_proposal_value is not None:
                self.proposed_value   = prev_proposal_value

        if len(self.promises_rcvd) == self.quorum_size:
            self.leader = True

            self.messenger.send_accept(self, self.proposal_id, self.proposed_value)
            
            return self.proposal_id, self.proposed_value



        
class Acceptor (object):

    messenger      = None
    
    promised_id    = None
    accepted_value = None
    accepted_id    = None
    previous_id    = None
    
    def recv_prepare(self, proposal_id):
        '''
        Returns: None on prepare failed. (proposal_id, promised_id, accepted_value) on success
        '''
        if proposal_id == self.promised_id:
            # Duplicate accepted proposal
            self.messenger.send_promise(self, proposal_id, self.previous_id, self.accepted_value)
            return proposal_id, self.previous_id, self.accepted_value
        
        if proposal_id > self.promised_id:
            self.previous_id = self.promised_id            
            self.promised_id = proposal_id
            self.messenger.send_promise(self, proposal_id, self.previous_id, self.accepted_value)
            return proposal_id, self.previous_id, self.accepted_value

        
    def recv_accept_request(self, proposal_id, value):
        '''
        Returns: None on request denied. (proposal_id, accepted_value) on accepted
        '''
        if proposal_id >= self.promised_id:
            self.accepted_value  = value
            self.promised_id     = proposal_id
            self.messenger.send_accepted(self, proposal_id, self.accepted_value)
            return proposal_id, self.accepted_value
        


        
class Learner (object):

    quorum_size       = None

    proposals         = None # maps proposal_id => [accept_count, retain_count, value]
    acceptors         = None # maps acceptor_uid => last_accepted_proposal_id
    final_value       = None
    final_proposal_id = None


    @property
    def complete(self):
        return self.final_proposal_id is not None


    def recv_accepted(self, acceptor_uid, proposal_id, accepted_value):
        '''
        Only messages from valid acceptors may result in calling this function.
        '''
        if self.final_value is not None:
            return self.final_value # already done

        if self.proposals is None:
            self.proposals = dict()
            self.acceptors = dict()
        
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
            self.final_value       = accepted_value
            self.final_proposal_id = proposal_id
            self.proposals         = None
            self.acceptors         = None

        self.messenger.on_resolution( self, proposal_id, accepted_value )
                                      
        return self.final_value
            


    
class PaxosNode (Proposer, Acceptor, Learner):
    '''
    This class supports the common model where each node on a network preforms
    all three Paxos roles, Proposer, Acceptor, and Learner.
    '''

    def __init__(self, messenger, node_uid, quorum_size, proposed_value=None):
        self.messenger   = messenger
        self.node_uid    = node_uid
        self.quorum_size = quorum_size

        if proposed_value is not None:
            self.set_proposal( proposed_value )
            


    def change_quorum_size(self, quorum_size):
        self.quorum_size

    

    def set_messenger(self, messenger):
        '''
        Required after unpickling a Node object to re-establish the
        messenger attribute
        '''
        self.messenger = messenger

        
    def __getstate__(self):
        pstate = dict( self.__dict__ )
        del pstate['messenger']
        return pstate

        
    def recv_prepare(self, proposal_id):
        self.observe_proposal( proposal_id )
        return super(PaxosNode,self).recv_prepare( proposal_id )
    
