
from paxos import basic


class Messenger (object):
    def send_prepare(self, proposer_obj, proposal_id):
        '''
        Sends a Prepare message to all nodes
        '''

    def send_accept(self, proposer_obj, proposal_id, proposal_value):
        '''
        Sends an Accept! message to all nodes
        '''


    def on_leadership_acquired(self, proposer_obj):
        '''
        Called by the Proposer object when leadership has been aquired
        '''

    def on_leadership_lost(self, proposer_obj):
        '''
        Called by the Proposer object when leadership has been lost
        '''

    


class Proposer (basic.Proposer):
    '''
    This class augments the basic Paxos Proposer to support an external
    mechanism for leader selection. This may be useful for cases where many
    instances of the Paxos algorithm are being run in parallel and leadership
    battles for each individual instance are best avoided. In this case, a
    dedicated instance of the Paxos algorithm (using a different Proposer
    implementation than this one) could be used to manage the election of a
    single leader. This leader would then drive the rest of the parallel
    instances to completion.

    This implementation does not modify the standard Paxos protocol any way and
    the leader-election portion of Phase1 must still complete as normal.
    '''

    
    def __init__(self, messenger, my_uid, quorum_size, proposed_value=None, leader_uid=None):

        super(Proposer, self).__init__(my_uid, quorum_size, proposed_value)

        self.messenger = messenger
        
        if self.proposer_uid == leader_uid:
            super(Proposer, self).prepare()
            self.leader = True



    def acquire_leadership(self):
        if self.proposal_id is None:
            super(Proposer, self).prepare()
        self.messenger.send_prepare( self, self.proposal_id )


    
    def set_proposal(self, value):
        '''
        Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted. 
        '''
        if self.value is None:
            self.value = value

            if self.leader:
                self.messenger.send_accept( self, self.proposal_id, self.value )


            
    def recv_promise(self, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):
        r = super(Proposer, self).recv_promise(acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value)

        if r:
            super(Proposer, self).prepare() # Increment proposal id for next leadership acquisition
            
            self.messenger.on_leadership_acquired(self)
            
            # If we have a value to propose, do so.
            if self.value is not None:
                self.messenger.send_accept( self, self.proposal_id, self.value )

        return r



    def recv_promise_nack(self, acceptor_uid, proposal_id, new_proposal_id):
        if proposal_id == self.proposal_id:
            self.observe_proposal( new_proposal_id )
            super(Proposer, self).prepare() # Increment proposal id for next leadership acquisition
        


    def recv_accept_nack(self, acceptor_uid, proposal_id, new_proposal_id):
        if proposal_id == self.proposal_id:
            self.observe_proposal( new_proposal_id )
            super(Proposer, self).prepare() # Increment proposal id for next leadership acquisition
            self.on_leadership_lost()
            


