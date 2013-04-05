package cocagne.paxos.essential;


public interface EssentialLearner {

	public boolean isComplete();
	
	public void receiveAccepted(String fromUID, ProposalID proposalID, Object acceptedValue);
	
	public Object getFinalValue();

	ProposalID getFinalProposalID();
}
