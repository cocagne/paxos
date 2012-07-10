
from paxos import basic, durable


class InvalidInstanceNumber (Exception):
    pass



    
@staticmethod
def basic_node_factory( proposer_uid, leader_uid, quorum_size, resolution_callback ):
    return basic.Node( basic.Proposer(proposer_uid, quorum_size),
                       basic.Acceptor(),
                       basic.Learner(quorum_size),
                       resolution_callback )

    
class MultiPaxos (object):
    '''
    This class ties a series of individual Paxos instances into a single, logical
    chain of values. This is done by assigning a sequence number to each
    instance of the Paxos algorithm and ignoring all function calls which do not
    match the current sequence number.

    The node_factory class attribute points to a factory function for basic.Node
    instances. A new Node instance will be created to handle each round of the
    Paxos algorithm. The purpose for the factory function is to allow custom
    extensions to the basic Paxos implementations of Proposer, Acceptor, and
    Learner. 
    '''
    
    node_factory = basic_node_factory

    def __init__(self, durable_dir=None, object_id=None):
        self.durable      = None
        self.uid          = None
        self.quorum_size  = None
        self.instance_num = None
        self.node         = None
        
        if durable_dir:
            self.durable = durable.DurableObjectHandler( durable_dir, object_id )
            
            if self.durable.serial != 1:
                d = self.durable.recovered
                
                self.uid          = d['uid']
                self.quorum_size  = d['quorum_size']
                self.instance_num = d['instance_num']
                self.node         = d['node']

                self.node.set_on_resolution_callback( self._on_resolution )

                self.onDurableRecover(d)
            

    def initialize(self, node_uid, quorum_size, instance_num=0):
                     
        self.uid          = node_uid
        self.quorum_size  = quorum_size
        self.instance_num = instance_num - 1
        self.node         = None

        self._next_instance()

        self._save_durable_state()

        
    def getDurableState(self):
        return {}

    
    def onDurableRecover(self, state):
        pass

    
    def _save_durable_state(self):
        if self.durable:
            d = dict( uid          = self.uid,
                      quorum_size  = self.quorum_size,
                      instance_num = self.instance_num,
                      node         = self.node )
            d.update( self.getDurableState() )
            self.durable.save( d )

        
    def _next_instance(self, leader_uid = None):
        self.instance_num += 1
        self.node          = self.node_factory( self.uid, leader_uid, self.quorum_size, self._on_resolution )
        

    def _on_resolution(self, proposal_id, value):
        inum = self.instance_num
        self._next_instance( proposal_id[1] )
        self._save_durable_state()
        self.on_proposal_resolution(inum, value)


    def on_proposal_resolution(self, instance_num, value):
        '''
        Called when a value for the current Paxos instance is chosend
        '''

    
    def set_instance_number(self, instance_number):
        self.instance_num = instance_number - 1
        self._next_instance()


    def change_quorum_size(self, new_quorum_size):
        self.quorum_size = new_quorum_size
        self.node.change_quorum_size(new_quorum_size)
        self._save_durable_state()
        

    def have_proposed_value(self):
        return self.node.proposer.value is not None

    def have_leadership(self):
        return self.node.proposer.leader

    # --- basic.Node wrappers ---

    def set_proposal(self, instance_num, value):
        if self.instance_num != instance_num:
            raise InvalidInstanceNumber()

        return self.node.set_proposal( value )
            
    def prepare(self):
        r = self.node.prepare()
        self._save_durable_state()
        return r

    def recv_promise(self, instance_num, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):
        if instance_num == self.instance_num:
            r = self.node.recv_promise(acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value)
            if r:
                self._save_durable_state()
            return r

    def recv_prepare(self, instance_num, proposal_id):
        if instance_num == self.instance_num:
            r = self.node.recv_prepare(proposal_id)
            if r:
                self._save_durable_state()
            return r

    def recv_accept_request(self, instance_num, proposal_id, value):
        if instance_num == self.instance_num:
            r = self.node.recv_accept_request(proposal_id, value)
            if r:
                self._save_durable_state()
            return r

    def recv_accepted(self, instance_num, acceptor_uid, proposal_id, accepted_value):
        if instance_num == self.instance_num:
            return self.node.recv_accepted(acceptor_uid, proposal_id, accepted_value)
