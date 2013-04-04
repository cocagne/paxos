package cocagne.paxos.practical;

import cocagne.paxos.essential.EssentialAcceptor;
import cocagne.paxos.essential.ProposalID;

public interface PracticalAcceptor extends EssentialAcceptor {

	public abstract boolean persistenceRequired();

	public abstract void recover(ProposalID promisedID, ProposalID acceptedID,
			Object acceptedValue);

	public abstract void persisted();

	public abstract boolean isActive();

	public abstract void setActive(boolean active);

}