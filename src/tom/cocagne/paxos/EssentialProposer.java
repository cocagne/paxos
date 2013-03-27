package tom.cocagne.paxos;

import java.util.HashSet;

public class EssentialProposer implements Proposer {
	
	EssentialMessenger  messenger;
    int                 proposerUID;
    int                 quorumSize;

    ProposalID          proposalID;
    Object              proposedValue      = null;
    ProposalID          lastAcceptedID     = null;
    HashSet<ProposalID> promisesReceived   = new HashSet<ProposalID>();
    
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
		// TODO Auto-generated method stub
		
	}

}
