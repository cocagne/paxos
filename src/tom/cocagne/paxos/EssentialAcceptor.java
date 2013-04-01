package tom.cocagne.paxos;

public class EssentialAcceptor implements Acceptor {
	
	protected EssentialMessenger messenger;
	protected ProposalID         promisedID;
	protected ProposalID         acceptedID;
	protected Object             acceptedValue;

	public EssentialAcceptor( EssentialMessenger messenger ) {
		this.messenger = messenger;
	}

	@Override
	public void receivePrepare(String fromUID, ProposalID proposalID) {
		
		if (this.promisedID != null && proposalID.equals(promisedID)) { // duplicate message
			messenger.sendPromise(fromUID, proposalID, acceptedID, acceptedValue);
		}
		else if (this.promisedID == null || proposalID.isGreaterThan(promisedID)) {
			promisedID = proposalID;
			messenger.sendPromise(fromUID, proposalID, acceptedID, acceptedValue);
		}
	}

	@Override
	public void receiveAcceptRequest(String fromUID, ProposalID proposalID,
			Object value) {
		if (promisedID == null || proposalID.isGreaterThan(promisedID) || proposalID.equals(promisedID)) {
			promisedID    = proposalID;
			acceptedID    = proposalID;
			acceptedValue = value;
			
			messenger.sendAccepted(acceptedID, acceptedValue);
		}
	}

	public EssentialMessenger getMessenger() {
		return messenger;
	}

	public ProposalID getPromisedID() {
		return promisedID;
	}

	public ProposalID getAcceptedID() {
		return acceptedID;
	}

	public Object getAcceptedValue() {
		return acceptedValue;
	}

}
