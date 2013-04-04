package cocagne.paxos.practical;

import cocagne.paxos.essential.EssentialProposerImpl;
import cocagne.paxos.essential.ProposalID;

public class PracticalProposerImpl extends EssentialProposerImpl implements PracticalProposer {

	private boolean leader = false;
	private boolean active = true;
	
	public PracticalProposerImpl(PracticalMessenger messenger, String proposerUID,
			int quorumSize) {
		super(messenger, proposerUID, quorumSize);
	}
	
	@Override
	public void setProposal(Object value) {
		if ( proposedValue == null ) {
			proposedValue = value;
			
			if (leader && active)
				messenger.sendAccept(proposalID, proposedValue);
		}
	}
	
	@Override
	public void prepare() {
		prepare(true);
	}
	
	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#prepare(boolean)
	 */
	@Override
	public void prepare( boolean incrementProposalNumber ) {
		if (incrementProposalNumber) {
			leader = false;
			
			promisesReceived.clear();
		
			proposalID.incrementNumber();
		}
		
		if (active)
			messenger.sendPrepare(proposalID);
	}
	
	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#observeProposal(java.lang.String, cocagne.paxos.essential.ProposalID)
	 */
	@Override
	public void observeProposal(String fromUID, ProposalID proposalID) {
		if (proposalID.isGreaterThan(this.proposalID))
			this.proposalID.setNumber(proposalID.getNumber());
	}
	
	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#receivePrepareNACK(java.lang.String, cocagne.paxos.essential.ProposalID, cocagne.paxos.essential.ProposalID)
	 */
	@Override
	public void receivePrepareNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID) {
		observeProposal(proposerUID, promisedID);
	}
	
	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#receiveAcceptNACK(java.lang.String, cocagne.paxos.essential.ProposalID, cocagne.paxos.essential.ProposalID)
	 */
	@Override
	public void receiveAcceptNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID) {
		
	}
	
	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#resendAccept()
	 */
	@Override
	public void resendAccept() {
		if (leader && active && proposedValue != null)
			messenger.sendAccept(proposalID, proposedValue);
	}
	
	@Override
	public void receivePromise(String fromUID, ProposalID proposalID,
			ProposalID prevAcceptedID, Object prevAcceptedValue) {
		
		observeProposal(fromUID, proposalID);
		
		if ( leader || !proposalID.equals(this.proposalID) || promisesReceived.contains(fromUID) ) 
			return;
		
        promisesReceived.add( fromUID );

        if (lastAcceptedID == null || prevAcceptedID.isGreaterThan(lastAcceptedID))
        {
        	lastAcceptedID = prevAcceptedID;

        	if (prevAcceptedValue != null)
        		proposedValue = prevAcceptedValue;
        }
        
        if (promisesReceived.size() == quorumSize) {
        	leader = true;
        	((PracticalMessenger)messenger).onLeadershipAcquired();
        	if (proposedValue != null && active)
        		messenger.sendAccept(this.proposalID, proposedValue);
        }
	}
	
	@Override
	public PracticalMessenger getMessenger() {
		return (PracticalMessenger) messenger;
	}

	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#isLeader()
	 */
	@Override
	public boolean isLeader() {
		return leader;
	}

	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#setLeader(boolean)
	 */
	@Override
	public void setLeader(boolean leader) {
		this.leader = leader;
	}

	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#isActive()
	 */
	@Override
	public boolean isActive() {
		return active;
	}

	/* (non-Javadoc)
	 * @see cocagne.paxos.practical.PracticalProposer#setActive(boolean)
	 */
	@Override
	public void setActive(boolean active) {
		this.active = active;
	}
}
