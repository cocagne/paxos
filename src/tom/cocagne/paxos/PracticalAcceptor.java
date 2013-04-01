package tom.cocagne.paxos;

public class PracticalAcceptor extends EssentialAcceptor {
	
	protected String  pendingAccepted = null;
	protected String  pendingPromise  = null;
	protected boolean active          = true;
	
	public PracticalAcceptor(PracticalMessenger messenger) {
		super(messenger);
	}

	public boolean persistenceRequired() {
		return pendingAccepted != null || pendingPromise != null;
	}
	
	public void recover(ProposalID promisedID, ProposalID acceptedID, Object acceptedValue) {
		this.promisedID    = promisedID;
		this.acceptedID    = acceptedID;
		this.acceptedValue = acceptedValue;
	}
	
	@Override
	public void receivePrepare(String fromUID, ProposalID proposalID) {
		if (this.promisedID != null && proposalID.equals(promisedID)) { // duplicate message
			if (active)
				messenger.sendPromise(fromUID, proposalID, acceptedID, acceptedValue);
		}
		else if (this.promisedID == null || proposalID.isGreaterThan(promisedID)) {
			if (pendingPromise == null) {
				promisedID = proposalID;
				if (active)
					pendingPromise = fromUID;
			}
		}
		else {
			if (active)
				((PracticalMessenger)messenger).sendPrepareNACK(fromUID, proposalID, promisedID);
		}
	}
	
	@Override
	public void receiveAcceptRequest(String fromUID, ProposalID proposalID,
			Object value) {
		if (acceptedID != null and proposalID.equals(acceptedID) && acceptedValue != null && acceptedValue.equals(value))
			if (active)
				messenger.sendAccepted(proposalID, value);
			
		if (promisedID == null || proposalID.isGreaterThan(promisedID) || proposalID.equals(promisedID)) {
			promisedID    = proposalID;
			acceptedID    = proposalID;
			acceptedValue = value;
			
			messenger.sendAccepted(acceptedID, acceptedValue);
		}
	}
	
	//messenger.sendPromise(fromUID, proposalID, acceptedID, acceptedValue);
}
