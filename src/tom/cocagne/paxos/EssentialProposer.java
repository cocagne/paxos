package tom.cocagne.paxos;

import java.util.HashSet;

public class EssentialProposer implements Proposer {
	
	private EssentialMessenger  messenger;
    private int                 proposerUID;
    private final int           quorumSize;

    private ProposalID          proposalID;
    private Object              proposedValue      = null;
    private ProposalID          lastAcceptedID     = null;
    private HashSet<Integer>    promisesReceived   = new HashSet<Integer>();
    
    public EssentialProposer(EssentialMessenger messenger, int proposerUID, int quorumSize) {
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
	public void receivePromise(int fromUID, ProposalID proposalID,
			ProposalID prevAcceptedID, Object prevAcceptedValue) {

		if ( !proposalID.equals(this.proposalID) || promisesReceived.contains(fromUID) ) 
			return;
		
        promisesReceived.add( fromUID );

        if (prevAcceptedID.isGreaterThan(lastAcceptedID))
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

	public int getProposerUID() {
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

}
