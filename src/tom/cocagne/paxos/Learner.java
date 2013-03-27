package tom.cocagne.paxos;

public interface Learner {

	public boolean isComplete();
	
	public void receiveAccepted(int from_uid, ProposalID proposalID, Object acceptedValue);
}
