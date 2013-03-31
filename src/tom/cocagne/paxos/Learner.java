package tom.cocagne.paxos;

public interface Learner {

	public boolean isComplete();
	
	public void receiveAccepted(String fromUID, ProposalID proposalID, Object acceptedValue);
}
