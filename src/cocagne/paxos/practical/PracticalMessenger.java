package cocagne.paxos.practical;

import cocagne.paxos.essential.EssentialMessenger;
import cocagne.paxos.essential.ProposalID;

public interface PracticalMessenger extends EssentialMessenger {
	
	public void sendPrepareNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID);
	
	public void sendAcceptNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID);
	
	public void onLeadershipAcquired();
}
