package tom.cocagne.paxos;

import java.util.HashSet;

public class HeartbeatNode extends PracticalNode {
	
	protected String          leaderUID;
	protected ProposalID      leaderProposalID;
	protected long            lastHeartbeatTimestamp;
	protected long            lastPrepareTimestamp;
	protected long            heartbeatPeriod         = 1000; // Milliseconds
	protected long            livenessWindow          = 5000; // Milliseconds
	protected boolean         acquiringLeadership     = false;
	protected HashSet<String> acceptNACKs             = new HashSet<String>();
	
	
	public HeartbeatNode(PracticalMessenger messenger, String proposerUID,
			int quorumSize, String leaderUID, int heartbeatPeriod, int livenessWindow) {
		super(messenger, proposerUID, quorumSize);
		
		this.leaderUID       = leaderUID;
		this.heartbeatPeriod = heartbeatPeriod;
		this.livenessWindow  = livenessWindow;
		
		leaderProposalID       = new ProposalID(1, leaderUID);
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

	private void acquireLeadership() {
		
		
	}
}
