
from paxos import basic


class InvalidInstanceNumber (Exception):
    pass



    
@staticmethod
def basic_node_factory( quorum_size, resolution_callback, multipaxos_obj ):
    return basic.Node( basic.Proposer(quorum_size),
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

        
    def _next_instance(self):
        self.instance_num += 1
        self.node          = self.node_factory( self.quorum_size, self._on_resolution, self )
        

    def _on_resolution(self, value):
        inum = self.instance_num
        self._next_instance()
        self.on_proposal_resolution(inum, value)


    def on_proposal_resolution(self, instance_num, value):
        pass

    
    def set_instance_number(self, instance_number):
        self.instance_num = instance_number - 1
        self._next_instance()


    # --- basic.Node wrappers ---

    def set_proposal(self, instance_num, value):
        if self.instance_num != instance_num:
            raise InvalidInstanceNumber()

        return self.node.set_proposal( value )
            
    def prepare(self):
        return self.node.prepare()

    def recv_promise(self, instance_num, acceptor_uid, proposal_number, prev_proposal_number, prev_proposal_value):
        if instance_num == self.instance_num:
            return self.node.recv_promise(acceptor_uid, proposal_number, prev_proposal_number, prev_proposal_value)

    def recv_prepare(self, instance_num, proposal_number):
        if instance_num == self.instance_num:
            return self.node.recv_prepare(proposal_number)

    def recv_accept_request(self, instance_num, proposal_number, value):
        if instance_num == self.instance_num:
            return self.node.recv_accept_request(proposal_number, value)

    def recv_accepted(self, instance_num, acceptor_uid, proposal_number, accepted_value):
        if instance_num == self.instance_num:
            return self.node.recv_accepted(acceptor_uid, proposal_number, accepted_value)
