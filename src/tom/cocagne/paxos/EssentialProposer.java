package tom.cocagne.paxos;

import java.util.HashSet;

public class EssentialProposer implements Proposer {
	
	protected EssentialMessenger  messenger;
    protected String              proposerUID;
    protected final int           quorumSize;

    protected ProposalID          proposalID;
    protected Object              proposedValue      = null;
    protected ProposalID          lastAcceptedID     = null;
    protected HashSet<String>     promisesReceived   = new HashSet<String>();
    
    public EssentialProposer(EssentialMessenger messenger, String proposerUID, int quorumSize) {
		this.messenger   = messenger;
		this.proposerUID = proposerUID;
		this.quorumSize  = quorumSize;
		this.proposalID  = new ProposalID(0, proposerUID);
	}

	@Override
	public void setProposal(Object value) {
		if ( proposedValue == null )
			proposedValue = value;
	}

	@Override
	public void prepare() {
		promisesReceived.clear();
		
		proposalID.incrementNumber();
		
		messenger.sendPrepare(proposalID);
	}

	@Override
	public void receivePromise(String fromUID, ProposalID proposalID,
			ProposalID prevAcceptedID, Object prevAcceptedValue) {

		if ( !proposalID.equals(this.proposalID) || promisesReceived.contains(fromUID) ) 
			return;
		
        promisesReceived.add( fromUID );

        if (lastAcceptedID == null || prevAcceptedID.isGreaterThan(lastAcceptedID))
        {
        	lastAcceptedID = prevAcceptedID;

        	if (prevAcceptedValue != null)
        		proposedValue = prevAcceptedValue;
        }
        
        if (promisesReceived.size() == quorumSize)
        	if (proposedValue != null)
        		messenger.sendAccept(this.proposalID, proposedValue);
	}

	public EssentialMessenger getMessenger() {
		return messenger;
	}

	public String getProposerUID() {
		return proposerUID;
	}

	public int getQuorumSize() {
		return quorumSize;
	}

	public ProposalID getProposalID() {
		return proposalID;
	}

	public Object getProposedValue() {
		return proposedValue;
	}

	public ProposalID getLastAcceptedID() {
		return lastAcceptedID;
	}
	
	public int numPromises() {
		return promisesReceived.size();
	}
}
