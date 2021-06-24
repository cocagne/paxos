'''
This module extends practical.Node to support external failure detection.
'''
from paxos import practical

from paxos.practical import ProposalID


class ExternalMessenger (practical.Messenger):

    def send_leadership_proclamation(self):
        '''
        Sends a leadership proclamation to all nodes
        '''
    
    def on_leadership_lost(self):
        '''
        Called when loss of leadership is detected
        '''

    def on_leadership_change(self, prev_leader_uid, new_leader_uid):
        '''
        Called when a change in leadership is detected. Either UID may
        be None.
        '''

        
    
class ExternalNode (practical.Node):
    '''
    This implementation is completely passive. An external entity must monitor peer nodes
    for failure and call prepare() when the node should attempt to acquire leadership
    of the Paxos instance. Continual polling of the prepare() function and prevention
    of eternal leadership battles is also the responsibility of the caller.

    When leadership is acquired, the node will broadcast a leadership proclamation,
    declaring itself the leader of the instance. This relieves peer nodes of the
    responsibility of tracking promises for all prepare messages.
    '''

    def __init__(self, messenger, my_uid, quorum_size, leader_uid=None):
        
        super(ExternalNode, self).__init__(messenger, my_uid, quorum_size)

        self.leader_uid          = leader_uid
        self.leader_proposal_id  = ProposalID(1, leader_uid)
        self._nacks              = set()

        if self.node_uid == leader_uid:
            self.leader                = True
            self.proposal_id           = ProposalID(self.next_proposal_number, self.node_uid)
            self.next_proposal_number += 1


    def prepare(self, *args, **kwargs):
        self._nacks.clear()
        return super(ExternalNode, self).prepare(*args, **kwargs)


    def recv_leadership_proclamation(self, from_uid, proposal_id):
        if proposal_id > self.leader_proposal_id:
            old_leader_uid = self.leader_uid
            
            self.leader_uid         = from_uid
            self.leader_proposal_id = proposal_id

            self.observe_proposal( from_uid, proposal_id )
            
            if old_leader_uid == self.node_uid:
                self.messenger.on_leadership_lost()
                
            self.messenger.on_leadership_change( old_leader_uid, from_uid )
        
        
    def recv_promise(self, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):

        pre_leader = self.leader
        
        super(ExternalNode, self).recv_promise(acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value)

        if not pre_leader and self.leader:
            old_leader_uid = self.leader_uid

            self.leader_uid         = self.node_uid
            self.leader_proposal_id = self.proposal_id
            
            self.messenger.send_leadership_proclamation( proposal_id )
            
            self.messenger.on_leadership_change( old_leader_uid, self.node_uid )

            
    def recv_accept_nack(self, from_uid, proposal_id, promised_id):
        if proposal_id == self.proposal_id:
            self._nacks.add(from_uid)

        if self.leader and len(self._nacks) >= self.quorum_size:
            self.leader             = False
            self.promises_rcvd      = set()
            self.leader_uid         = None
            self.leader_proposal_id = None
            self.messenger.on_leadership_lost()
            self.messenger.on_leadership_change(self.node_uid, None)
            self.observe_proposal( from_uid, promised_id )

