package tom.cocagne.paxos;

public interface PracticalMessenger extends EssentialMessenger {
	
	public void sendPrepareNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID);
	
	public void sendAcceptNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID);
	
	public void onLeadershipAcquired();
}
