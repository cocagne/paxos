
from paxos import basic


class InvalidInstanceNumber (Exception):
    pass



    
@staticmethod
def basic_node_factory( proposer_uid, leader_uid, quorum_size, resolution_callback ):
    return basic.Node( basic.Proposer(proposer_uid, quorum_size),
                       basic.Acceptor(),
                       basic.Learner(quorum_size),
                       resolution_callback )

    
class MultiPaxos (object):

    node_factory = basic_node_factory
    
    def __init__(self, node_uid, quorum_size, instance_num=1 ):
        self.uid          = node_uid
        self.quorum_size  = quorum_size
        self.instance_num = instance_num - 1
        self.node         = None

        self._next_instance()

        
    def _next_instance(self, leader_uid = None):
        self.instance_num += 1
        self.node          = self.node_factory( self.uid, leader_uid, self.quorum_size, self._on_resolution )
        

    def _on_resolution(self, proposal_id, value):
        inum = self.instance_num
        self._next_instance( proposal_id[1] )
        self.on_proposal_resolution(inum, value)


    def on_proposal_resolution(self, instance_num, value):
        pass

    
    def set_instance_number(self, instance_number):
        self.instance_num = instance_number - 1
        self._next_instance()


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
        return self.node.prepare()

    def recv_promise(self, instance_num, acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value):
        if instance_num == self.instance_num:
            return self.node.recv_promise(acceptor_uid, proposal_id, prev_proposal_id, prev_proposal_value)

    def recv_prepare(self, instance_num, proposal_id):
        if instance_num == self.instance_num:
            return self.node.recv_prepare(proposal_id)

    def recv_accept_request(self, instance_num, proposal_id, value):
        if instance_num == self.instance_num:
            return self.node.recv_accept_request(proposal_id, value)

    def recv_accepted(self, instance_num, acceptor_uid, proposal_id, accepted_value):
        if instance_num == self.instance_num:
            return self.node.recv_accepted(acceptor_uid, proposal_id, accepted_value)
