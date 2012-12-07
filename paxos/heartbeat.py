
import time

from paxos import node


class HeartbeatMessenger (node.Messenger):

    def send_heartbeat(self, node_obj, leader_proposal_id):
        '''
        Sends a heartbeat message to all nodes
        '''

    def schedule(self, node_obj,  msec_delay, func_obj):
        '''
        Called by pulse() to schedule the next pulse() call while this node has
        leadership. If this method is not overridden appropriately, subclasses
        must use the on_leadership_acquired()/on_leadership_lost() callbacks
        to ensure that pulse() is called every hb_period while leadership is held.
        '''

    def on_leadership_lost(self, node_obj):
        '''
        Called when loss of leadership is detected
        '''

    def on_leadership_change(self, node_obj, prev_leader_uid, new_leader_uid):
        '''
        Called when a change in leadership is detected
        '''
    

class HeartbeatPaxosNode (node.PaxosNode):
    '''
    This class augments the basic Paxos node to provide a reasonable
    assurance of progress through a heartbeat mechanism used to detect leader
    failure and initiate leadership acquisition.

    If one or more heartbeat messages are not received within the
    'liveness_window', leadership acquisition will be attempted by sending out
    phase 1a, Prepare messages. If a quorum of replies acknowledging leadership
    is received, the node has successfully gained leadership and will begin
    sending out heartbeat messages itself. If a quorum is not received, the
    node will continually resend its proposal every 'liveness_window' until either
    a quorum is established or a heartbeat with a proposal number greater than
    its own is seen. The units for hb_period and liveness_window is seconds. Floating
    point values may be used for sub-second precision.

    Leadership loss is detected by way of receiving a heartbeat message from a proposer
    with a higher proposal number (which must be obtained through a successful phase 1).
    Or by receiving a quorum of NACK responses to Accept! messages.

    This process does not modify the basic Paxos algorithm in any way, it merely seeks
    to ensure recovery from failures in leadership. Consequently, the basic Paxos
    safety mechanisms remain intact.
    '''

    hb_period       = 1
    liveness_window = 5

    timestamp       = time.time

    
    def __init__(self, messenger, my_uid, quorum_size, proposed_value=None, leader_uid=None,
                 hb_period=None, liveness_window=None):
        
        super(HeartbeatPaxosNode, self).__init__(messenger, my_uid, quorum_size, proposed_value)

        self.leader_proposal_id  = (1, leader_uid)
        self._tlast              = self.timestamp()
        self._acquiring          = False
        self._nacks              = set()

        if hb_period:       self.hb_period       = hb_period
        if liveness_window: self.liveness_window = liveness_window

        if self.node_uid == leader_uid:
            self.leader = True


    @property
    def current_leader_uid(self):
        return self.leader_proposal_id[1] if self.leader_proposal_id is not None else None



    def on_recover(self, messenger):
        '''
        Must be called after the instance has been recovered from durable state
        '''
        super(HeartbeatPaxosNode, self).on_recover(messenger)
        self.leader_proposal_id = (0,None)


            
    def prepare(self):
        self._nacks.clear()
        return super(HeartbeatPaxosNode, self).prepare()
        

        
    def leader_is_alive(self):
        return self.timestamp() - self._tlast <= self.liveness_window


    
    def poll_liveness(self):
        '''
        Should be called every liveness_window
        '''
        if self._acquiring:
            self.messenger.send_prepare( self, self.proposal_id )
            
        elif not self.leader_is_alive():
            self.acquire_leadership()


            
    def recv_heartbeat(self, proposal_id):
        leader_proposal_number, node_uid = proposal_id

        if proposal_id > self.leader_proposal_id:
            # Change of leadership            
            self._acquiring = False
            
            old_leader_uid = self.leader_proposal_id[1] if self.leader_proposal_id is not None else None
            
            self.leader_proposal_id = proposal_id

            if self.leader and proposal_id[1] != self.node_uid:
                self.leader = False
                self.messenger.on_leadership_lost(self)
                self.observe_proposal( proposal_id )

            self.messenger.on_leadership_change( self, old_leader_uid, proposal_id[1] )            

        if self.leader_proposal_id == proposal_id:
            self._tlast = self.timestamp()
                

            
    def pulse(self):
        '''
        Must be called every hb_period while this node is the leader
        '''
        if self.leader:
            self.recv_heartbeat(self.proposal_id)
            self.messenger.send_heartbeat(self, self.proposal_id)
            self.messenger.schedule(self, self.hb_period, self.pulse)


            
    def acquire_leadership(self):
        if self.leader_is_alive():
            self._acquiring = False

        else:
            self._acquiring = True
            self.prepare()


        
    def recv_promise(self, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):

        pre_leader = self.leader
        
        super(HeartbeatPaxosNode, self).recv_promise(acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value)

        if not pre_leader and self.leader:
            old_leader_uid = self.leader_proposal_id[1] if self.leader_proposal_id is not None else None
            
            self.leader_proposal_id = self.proposal_id
            self._acquiring         = False
            self.pulse()
            self.messenger.on_leadership_change( self, old_leader_uid, proposal_id[1] )
            


    def recv_accept_nack(self, acceptor_uid, proposal_id, new_proposal_id):
        if proposal_id == self.proposal_id:
            self._nacks.add(acceptor_uid)

        if self.leader and len(self._nacks) >= self.quorum_size:
            self.leader_proposal_id = None
            self.messenger.on_leadership_lost(self)
            self.messenger.on_leadership_change(self, self.node_uid, None)
            self.observe_proposal( new_proposal_id )


    
