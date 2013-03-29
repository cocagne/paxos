package tom.cocagne.paxos;

public class EssentialAcceptor implements Acceptor {
	
	private EssentialMessenger messenger;
	private ProposalID         promisedID;
	private ProposalID         acceptedID;
	private Object             acceptedValue;

	public EssentialAcceptor( EssentialMessenger messenger ) {
		this.messenger = messenger;
	}

	@Override
	public void receivePrepare(int fromUID, ProposalID proposalID) {
		
		if (proposalID.equals(promisedID)) { // duplicate message
			messenger.sendPromise(fromUID, proposalID, acceptedID, acceptedValue);
		}
		else if (proposalID.isGreaterThan(promisedID)) {
			promisedID = proposalID;
			messenger.sendPromise(fromUID, proposalID, acceptedID, acceptedValue);
		}
	}

	@Override
	public void receiveAcceptRequest(int fromUID, ProposalID proposalID,
			Object value) {
		if (proposalID.isGreaterThan(promisedID) || proposalID.equals(promisedID)) {
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
