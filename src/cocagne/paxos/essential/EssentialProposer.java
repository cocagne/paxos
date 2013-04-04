package cocagne.paxos.essential;


public interface EssentialProposer {
	
	public void setProposal(Object value);
	
	public void prepare();
	
	public void receivePromise(String fromUID, ProposalID proposalID, ProposalID prevAcceptedID, Object prevAcceptedValue);

}
