package tom.cocagne.paxos;

public class PracticalProposer extends EssentialProposer {

	private boolean leader = false;
	private boolean active = true;
	
	public PracticalProposer(PracticalMessenger messenger, String proposerUID,
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
	
	public void prepare( boolean incrementProposalNumber ) {
		if (incrementProposalNumber) {
			leader = false;
			
			promisesReceived.clear();
		
			proposalID.incrementNumber();
		}
		
		if (active)
			messenger.sendPrepare(proposalID);
	}
	
	public void observeProposal(String fromUID, ProposalID proposalID) {
		if (proposalID.isGreaterThan(this.proposalID))
			this.proposalID.setNumber(proposalID.getNumber() + 1);
	}
	
	public void receivePrepareNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID) {
		observeProposal(proposerUID, promisedID);
	}
	
	public void receiveAcceptNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID) {
		
	}
	
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
        	if (proposedValue != null)
        		messenger.sendAccept(this.proposalID, proposedValue);
        }
	}
}
