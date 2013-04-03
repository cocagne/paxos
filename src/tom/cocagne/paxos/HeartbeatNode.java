package tom.cocagne.paxos;

import java.util.HashSet;

public class HeartbeatNode extends PracticalNode {
	
	protected HeartbeatMessenger messenger;
	protected String             leaderUID;
	protected ProposalID         leaderProposalID;
	protected long               lastHeartbeatTimestamp;
	protected long               lastPrepareTimestamp;
	protected long               heartbeatPeriod         = 1000; // Milliseconds
	protected long               livenessWindow          = 5000; // Milliseconds
	protected boolean            acquiringLeadership     = false;
	protected HashSet<String>    acceptNACKs             = new HashSet<String>();
	
	
	public HeartbeatNode(HeartbeatMessenger messenger, String proposerUID,
			int quorumSize, String leaderUID, int heartbeatPeriod, int livenessWindow) {
		super(messenger, proposerUID, quorumSize);
		
		this.messenger       = messenger;
		this.leaderUID       = leaderUID;
		this.heartbeatPeriod = heartbeatPeriod;
		this.livenessWindow  = livenessWindow;
		
		leaderProposalID       = null;
		lastHeartbeatTimestamp = timestamp();
		lastPrepareTimestamp   = timestamp();
		
		if (leaderUID != null && proposerUID.equals(leaderUID))
			setLeader(true);
	}
	
	public long timestamp() {
		return System.currentTimeMillis();
	}

	@Override
	public void prepare(boolean incrementProposalNumber) {
		if (incrementProposalNumber)
			acceptNACKs.clear();
		super.prepare(incrementProposalNumber);
	}
	
	public boolean leaderIsAlive() {
		return timestamp() - lastHeartbeatTimestamp <= livenessWindow;
	}
	
	public boolean observedRecentPrepare() {
		return timestamp() - lastPrepareTimestamp <= livenessWindow * 1.5;
	}
	
	public void pollLiveness() {
		if (!leaderIsAlive() && !observedRecentPrepare()) {
			if (acquiringLeadership)
				prepare();
			else
				acquireLeadership();
		}
	}
	
	public void receiveHeartbeat(String fromUID, ProposalID proposalID) {
		
		if (leaderProposalID == null || proposalID.isGreaterThan(leaderProposalID)) {
			acquiringLeadership = false;
			String oldLeaderUID = leaderUID;
			
			leaderUID        = fromUID;
			leaderProposalID = proposalID;
			
			if (isLeader() && !fromUID.equals(getProposerUID())) {
				setLeader(false);
				messenger.onLeadershipLost();
				observeProposal(fromUID, proposalID);
			}
			
			messenger.onLeadershipChange(oldLeaderUID, fromUID);
		}
		
		if (leaderProposalID != null && leaderProposalID.equals(proposalID))
			lastHeartbeatTimestamp = timestamp();
	}
	
	public void pulse() {
		if (isLeader()) {
			receiveHeartbeat(getProposerUID(), getProposalID());
			messenger.sendHeartbeat(getProposalID());
			messenger.schedule(heartbeatPeriod, new HeartbeatCallback () { 
				public void execute() { pulse(); }
			});
		}
	}

	private void acquireLeadership() {
		if (leaderIsAlive())
			acquiringLeadership = false;
		else {
			acquiringLeadership = true;
			prepare();
		}
	}
	
	@Override
	public void receivePrepare(String fromUID, ProposalID proposalID) {
		super.receivePrepare(fromUID, proposalID);
		if (!proposalID.equals(getProposalID()))
			lastPrepareTimestamp = timestamp();
	}
	
	@Override
	public void receivePromise(String fromUID, ProposalID proposalID,
			ProposalID prevAcceptedID, Object prevAcceptedValue) {
		String preLeaderUID = leaderUID;
		
		super.receivePromise(fromUID, proposalID, prevAcceptedID, prevAcceptedValue);
		
		if (preLeaderUID == null && isLeader()) {
			String oldLeaderUID = getProposerUID();
			
			leaderUID           = getProposerUID();
			leaderProposalID    = getProposalID();
			acquiringLeadership = false;
			
			pulse();
			
			messenger.onLeadershipChange(oldLeaderUID, leaderUID);
		}
	}
	
	@Override
	public void receivePrepareNACK(String proposerUID, ProposalID proposalID,
			ProposalID promisedID) {
		super.receivePrepareNACK(proposerUID, proposalID, promisedID);
		
		if (acquiringLeadership)
			prepare();
	}
	
	@Override
	public void receiveAcceptNACK(String proposerUID, ProposalID proposalID,
			ProposalID promisedID) {
		super.receiveAcceptNACK(proposerUID, proposalID, promisedID);
		
		if (proposalID.equals(getProposalID()))
			acceptNACKs.add(proposerUID);
		
		if (isLeader() && acceptNACKs.size() >= getQuorumSize()) {
			setLeader(false);
			leaderUID        = null;
			leaderProposalID = null;
			messenger.onLeadershipLost();
			messenger.onLeadershipChange(getProposerUID(), null);
			observeProposal(proposerUID, proposalID);
		}
	}
}
