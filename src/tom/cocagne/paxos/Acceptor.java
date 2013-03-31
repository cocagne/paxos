package tom.cocagne.paxos;

public interface Acceptor {
	public void receivePrepare(String fromUID, ProposalID proposalID);
	
	public void receiveAcceptRequest(String fromUID, ProposalID proposalID, Object value);
}
