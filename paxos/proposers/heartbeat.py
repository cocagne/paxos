
import time

from paxos import basic

class Proposer (basic.Proposer):
    '''
    This class augments the basic Paxos Proposer to provide a reasonable
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

    This process does not modify the basic Paxos algorithm in any way, it merely seeks
    to ensure recovery from failures in leadership. Consequently, the basic Paxos
    safety mechanisms remain intact.
    '''

    hb_period       = 1
    liveness_window = 5

    timestamp       = time.time

    #------------------------------
    # Subclass API
    #
    def send_prepare(self, proposal_id):
        raise NotImplementedError

    def send_accept(self, proposal_id, proposal_value):
        raise NotImplementedError

    def send_heartbeat(self, leader_proposal_id):
        raise NotImplementedError

    def schedule(self, msec_delay, func_obj):
        '''
        Called by pulse() to schedule the next pulse() call while this node has
        leadership. If this method is not overridden appropriately, subclasses
        must use the on_leadership_acquired()/on_leadership_lost() callbacks
        to ensure that pulse() is called every hb_period while leadership is held.
        '''
        pass

    def on_leadership_acquired(self):
        pass

    def on_leadership_lost(self):
        pass

    def on_leadership_change(self, prev_leader_uid, new_leader_uid):
        pass
    #------------------------------
    
    def __init__(self, my_uid, quorum_size, proposed_value=None, leader_uid=None,
                 hb_period=None, liveness_window=None):
        super(Proposer, self).__init__(my_uid, quorum_size, proposed_value)

        self.proposal_id         = (1, self.proposer_uid)
        self.leader_proposal_id  = (1, leader_uid)
        self._tlast              = self.timestamp()
        self._acquiring          = None # holds proposal id for our leadership request

        if hb_period:       self.hb_period       = hb_period
        if liveness_window: self.liveness_window = liveness_window

        if self.proposer_uid == leader_uid:
            self.leader = True
        

        
    def leader_is_alive(self):
        return self.timestamp() - self._tlast <= self.liveness_window


    
    def poll_liveness(self):
        '''
        Should be called every liveness_window
        '''
        if self._acquiring:
            # XXX Could add a random delay here to reduce the chance of collisions
            self.send_prepare( self._acquiring )
            
        elif not self.leader_is_alive():
            self.acquire_leadership()


            
    def set_proposal(self, value):
        '''
        Sets the proposal value for this node iff this node is not already aware of
        another proposal having already been accepted. 
        '''
        if self.value is None:
            self.value = value

            if self.leader:
                self.send_accept( self.proposal_id, self.value )


            
    def recv_heartbeat(self, proposal_id):
        leader_proposal_number, node_uid = proposal_id
        
        if proposal_id > self.leader_proposal_id:
            # Change of leadership            
            self._acquiring = None
            
            old_leader_uid = self.leader_proposal_id[1] if self.leader_proposal_id is not None else None
            
            self.leader_proposal_id = proposal_id

            if self.leader and proposal_id[1] != self.proposer_uid:
                self.leader = False
                self.on_leadership_lost()
                self.observe_proposal( proposal_id )

            self.on_leadership_change( old_leader_uid, proposal_id[1] )

            

        if self.leader_proposal_id == proposal_id:
            self._tlast = self.timestamp()
                

            
    def pulse(self):
        '''
        Must be called every hb_period while this node is the leader
        '''
        if self.leader:
            self.recv_heartbeat( self.proposal_id )
            self.send_heartbeat( self.proposal_id )
            self.schedule(self.hb_period, self.pulse)


            
    def acquire_leadership(self):
        if self.leader_is_alive():
            self._acquiring = None

        else:
            self._acquiring = self.prepare()
            self.send_prepare( self._acquiring )


        
    def recv_promise(self, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):
        r = super(Proposer, self).recv_promise(acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value)

        if r:
            old_leader_uid = self.leader_proposal_id[1] if self.leader_proposal_id is not None else None
            
            self.leader_proposal_id = self.proposal_id
            self._acquiring         = None
            self.pulse()
            self.on_leadership_acquired()
            self.on_leadership_change( old_leader_uid, proposal_id[1] )
            
            # If we have a value to propose, do so.
            if self.value is not None:
                self.send_accept( self.proposal_id, self.value )

        return r


    
    #def recv_proposal_rejected(self, acceptor_uid, proposal_id):
    #    if proposal_id > self._acquiring:
    #        self._acquiring = None
        
