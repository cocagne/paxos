package tom.cocagne.paxos;

public interface Proposer {
	
	public void setProposal(Object value);
	
	public void prepare();
	
	public void receivePromise(String fromUID, ProposalID proposalID, ProposalID prevAcceptedID, Object prevAcceptedValue);

}
