package cocagne.paxos.practical;

import cocagne.paxos.essential.EssentialLearner;
import cocagne.paxos.essential.EssentialLearnerImpl;
import cocagne.paxos.essential.ProposalID;

public class PracticalNode implements PracticalProposer, PracticalAcceptor, EssentialLearner {

	protected PracticalProposerImpl proposer;
	protected PracticalAcceptorImpl acceptor;
	protected EssentialLearnerImpl  learner;
	
	public PracticalNode(PracticalMessenger messenger, String proposerUID,
			int quorumSize) {
		
		proposer = new PracticalProposerImpl(messenger, proposerUID, quorumSize);
		acceptor = new PracticalAcceptorImpl(messenger);
		learner  = new EssentialLearnerImpl(messenger, quorumSize);
	}
	
	public boolean isActive() {
		return proposer.isActive();
	}

	public void setActive(boolean active) {
		proposer.setActive(active);
		acceptor.setActive(active);
	}

	//-------------------------------------------------------------------------
	// Learner
	//
	@Override
	public boolean isComplete() {
		return learner.isComplete();
	}

	@Override
	public void receiveAccepted(String fromUID, ProposalID proposalID,
			Object acceptedValue) {
		learner.receiveAccepted(fromUID, proposalID, acceptedValue);

	}
	
	@Override
	public Object getFinalValue() {
		return learner.getFinalValue();
	}

	@Override
	public ProposalID getFinalProposalID() {
		return learner.getFinalProposalID();
	}

	//-------------------------------------------------------------------------
	// Acceptor
	//
	@Override
	public void receivePrepare(String fromUID, ProposalID proposalID) {
		acceptor.receivePrepare(fromUID, proposalID);
	}

	@Override
	public void receiveAcceptRequest(String fromUID, ProposalID proposalID,
			Object value) {
		acceptor.receiveAcceptRequest(fromUID, proposalID, value);
	}
	
	public ProposalID getPromisedID() {
		return acceptor.getPromisedID();
	}

	public ProposalID getAcceptedID() {
		return acceptor.getAcceptedID();
	}

	public Object getAcceptedValue() {
		return acceptor.getAcceptedValue();
	}
	
	public boolean persistenceRequired() {
		return acceptor.persistenceRequired();
	}
	
	public void recover(ProposalID promisedID, ProposalID acceptedID, Object acceptedValue) {
		acceptor.recover(promisedID, acceptedID, acceptedValue);
	}
	
	public void persisted() {
		acceptor.persisted();
	}

	//-------------------------------------------------------------------------
	// Proposer
	//
	@Override
	public void setProposal(Object value) {
		proposer.setProposal(value);
	}

	@Override
	public void prepare() {
		proposer.prepare();
	}
	
	public void prepare( boolean incrementProposalNumber ) {
		proposer.prepare(incrementProposalNumber);
	}

	@Override
	public void receivePromise(String fromUID, ProposalID proposalID,
			ProposalID prevAcceptedID, Object prevAcceptedValue) {
		proposer.receivePromise(fromUID, proposalID, prevAcceptedID, prevAcceptedValue);
	}
	
	public PracticalMessenger getMessenger() {
		return proposer.getMessenger();
	}

	public String getProposerUID() {
		return proposer.getProposerUID();
	}

	public int getQuorumSize() {
		return proposer.getQuorumSize();
	}

	public ProposalID getProposalID() {
		return proposer.getProposalID();
	}

	public Object getProposedValue() {
		return proposer.getProposedValue();
	}

	public ProposalID getLastAcceptedID() {
		return proposer.getLastAcceptedID();
	}
	
	public int numPromises() {
		return proposer.numPromises();
	}
	
	public void observeProposal(String fromUID, ProposalID proposalID) {
		proposer.observeProposal(fromUID, proposalID);
	}
	
	public void receivePrepareNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID) {
		proposer.receivePrepareNACK(proposerUID, proposalID, promisedID);
	}
	
	public void receiveAcceptNACK(String proposerUID, ProposalID proposalID, ProposalID promisedID) {
		proposer.receiveAcceptNACK(proposerUID, proposalID, promisedID);
	}
	
	public void resendAccept() {
		proposer.resendAccept();
	}
	
	public boolean isLeader() {
		return proposer.isLeader();
	}

	public void setLeader(boolean leader) {
		proposer.setLeader(leader);
	}
}
