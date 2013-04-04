package cocagne.paxos.essential;


public interface EssentialAcceptor {
	public void receivePrepare(String fromUID, ProposalID proposalID);
	
	public void receiveAcceptRequest(String fromUID, ProposalID proposalID, Object value);
}
