from paxos import essential

from paxos.essential import ProposalID


class Messenger (essential.Messenger):
    
    def send_prepare_nack(self, to_uid, proposal_id, promised_id):
        '''
        Sends a Prepare Nack message for the proposal to the specified node
        '''

    def send_accept_nack(self, to_uid, proposal_id, promised_id):
        '''
        Sends a Accept! Nack message for the proposal to the specified node
        '''

    def on_leadership_acquired(self):
        '''
        Called when leadership has been aquired. This is not a guaranteed
        position. Another node may assume leadership at any time and it's
        even possible that another may have successfully done so before this
        callback is exectued. Use this method with care.

        The safe way to guarantee leadership is to use a full Paxos instance
        whith the resolution value being the UID of the leader node. To avoid
        potential issues arising from timing and/or failure, the election
        result may be restricted to a certain time window. Prior to the end of
        the window the leader may attempt to re-elect itself to extend it's
        term in office.
        '''

        
class Proposer (essential.Proposer):
    
    leader = False

    
    def set_proposal(self, value):
        '''
        Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted. 
        '''
        if self.proposed_value is None:
            self.proposed_value = value

            if self.leader:
                self.messenger.send_accept( self.proposal_id, value )


    def prepare(self, increment_proposal_number=True):
        '''
        Sends a prepare request to all Acceptors as the first step in attempting to
        acquire leadership of the Paxos instance. If the default argument is True,
        the proposal id will be set higher than that of any previous observed
        proposal id. Otherwise the previously used proposal id will simply be
        retransmitted.
        
        The proposal id is a tuple of (proposal_numer, proposer_uid)
        '''
        if increment_proposal_number:
            self.leader        = False
            self.promises_rcvd = set()
            self.proposal_id   = (self.next_proposal_number, self.proposer_uid)
        
            self.next_proposal_number += 1

        self.messenger.send_prepare(self.proposal_id)

    
    def observe_proposal(self, from_uid, proposal_id):
        '''
        Optional method used to update the proposal counter as proposals are seen on the network.
        When co-located with Acceptors and/or Learners, this method may be used to avoid a message
        delay when attempting to assume leadership (guaranteed NACK if the proposal number is too low).
        '''
        if from_uid != self.proposer_uid:
            if proposal_id >= (self.next_proposal_number, self.proposer_uid):
                self.next_proposal_number = proposal_id.number + 1

            
    def recv_prepare_nack(self, from_uid, proposal_id, promised_id):
        '''
        Called when an explicit NACK is sent in response to a prepare message.
        '''
        self.observe_proposal( from_uid, promised_id )

    
    def recv_accept_nack(self, from_uid, proposal_id, promised_id):
        '''
        Called when an explicit NACK is sent in response to an accept message
        '''

        
    def resend_accept(self):
        '''
        Retransmits an Accept! message iff this node is the leader and has
        a proposal value
        '''
        if self.leader and self.proposed_value:
            self.messenger.send_accept(self.proposal_id, self.proposed_value)


    def recv_promise(self, from_uid, proposal_id, prev_accepted_id, prev_accepted_value):
        '''
        Called when a Promise message is received from the network
        '''
        self.observe_proposal( from_uid, proposal_id )

        if self.leader or proposal_id != self.proposal_id or from_uid in self.promises_rcvd:
            return

        self.promises_rcvd.add( from_uid )
        
        if prev_accepted_id > self.last_accepted_id:
            self.last_accepted_id = prev_accepted_id
            # If the Acceptor has already accepted a value, we MUST set our proposal
            # to that value. Otherwise, we may retain our current value.
            if prev_accepted_value is not None:
                self.proposed_value = prev_accepted_value

        if len(self.promises_rcvd) == self.quorum_size:
            self.leader = True

            self.messenger.on_leadership_acquired()
            
            if self.proposed_value is not None:
                self.messenger.send_accept(self.proposal_id, self.proposed_value)


                
class Acceptor (essential.Acceptor):
    '''
    Acceptors act as the fault-tolerant memory for Paxos. To ensure correctness
    in the presense of failure, Acceptors must be able to remember the promises
    they've made even in the event of power outages. Consequently, any changes to
    the promised_id and/or accepted_value must be persisted to stable media prior
    to sending promise and accepted messages.

    Note that because Paxos permits any combination of dropped packages, not
    every promise/accepted message needs to be sent. This implementation only
    responds to the last prepare/accept_request message received prior to
    saving the Acceptor's values to stable media. After saving the promised_id
    and accepted_value variables, the "state_saved" method must be called to
    send the pending promise and/or accepted messages.
    '''

    pending_promise  = None # None or the UID to send a promise message to
    pending_accepted = None # None or the UID to send an accepted message to

    
    @property
    def state_save_required(self):
        return self.pending_promise is not None or self.pending_accepted is not None
    

    def recv_prepare(self, from_uid, proposal_id):
        '''
        Called when a Prepare message is received from the network
        '''
        if proposal_id == self.promised_id:
            # Duplicate prepare message. No change in state is necessary so the response
            # may be sent immediately
            self.messenger.send_promise(from_uid, proposal_id, self.previous_id, self.accepted_value)
        
        elif proposal_id > self.promised_id:
            self.previous_id = self.promised_id            
            self.promised_id = proposal_id
            self.pending_promise = from_uid

        else:
            self.messenger.send_prepare_nack(from_uid, proposal_id, self.promised_id)

                    
    def recv_accept_request(self, from_uid, proposal_id, value):
        '''
        Called when an Accept! message is received from the network
        '''
        if proposal_id == self.promised_id and value == self.accepted_value:
            # Duplicate accepted proposal. No change in state is necessary so the response
            # may be sent immediately
            self.messenger.send_accepted(proposal_id, value)
            
        elif proposal_id >= self.promised_id:
            self.accepted_value   = value
            self.promised_id      = proposal_id
            self.pending_accepted = from_uid
            
        else:
            self.messenger.send_accept_nack(from_uid, proposal_id, self.promised_id)


    def state_saved(self):
        if self.pending_promise:
            self.messenger.send_promise(self.pending_promise,
                                        self.promised_id,
                                        self.previous_id,
                                        self.accepted_value)
            
        if self.pending_accepted:
            self.messenger.send_accepted(self.promised_id,
                                         self.accepted_value)
        
        self.pending_promise  = None
        self.pending_accepted = None


        
class Learner (essential.Learner):
    '''
    No additional functionality required.
    '''
            

    
class Node (Proposer, Acceptor, Learner):
    '''
    This class supports the common model where each node on a network preforms
    all three Paxos roles, Proposer, Acceptor, and Learner.
    '''

    def __init__(self, messenger, node_uid, quorum_size):
        self.messenger   = messenger
        self.node_uid    = node_uid
        self.quorum_size = quorum_size


    @property
    def proposer_uid(self):
        return self.node_uid
            

    def __getstate__(self):
        pstate = dict( self.__dict__ )
        del pstate['messenger']
        return pstate

    
    def recover(self, messenger):
        '''
        Required after unpickling a Node object to re-establish the
        messenger attribute
        '''
        self.messenger = messenger

        
    def change_quorum_size(self, quorum_size):
        self.quorum_size = quorum_size

        
    def recv_prepare(self, from_uid, proposal_id):
        self.observe_proposal( from_uid, proposal_id )
        return super(Node,self).recv_prepare( from_uid, proposal_id )


