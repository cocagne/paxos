package tom.cocagne.paxos;

public interface Acceptor {
	public void receivePrepare(int fromUID, ProposalID proposalID);
	
	public void receiveAcceptRequest(int fromUID, ProposalID proposalID, Object value);
}
