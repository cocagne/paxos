package tom.cocagne.paxos;

public interface Learner {

	public boolean isComplete();
	
	public void receiveAccepted(int fromUID, ProposalID proposalID, Object acceptedValue);
}
