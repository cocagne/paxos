package tom.cocagne.paxos;

public class ProposalID {
	

	private int       number;
	private final int uid;
	
	public ProposalID(int number, int uid) {
		this.number = number;
		this.uid    = uid;
	}

	public int getNumber() {
		return number;
	}
	
	public void setNumber(int number) {
		this.number = number;
	}
	
	public void incrementNumber() {
		this.number += 1;
	}

	public int getUid() {
		return uid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + number;
		result = prime * result + uid;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProposalID other = (ProposalID) obj;
		if (number != other.number)
			return false;
		if (uid != other.uid)
			return false;
		return true;
	}
}
