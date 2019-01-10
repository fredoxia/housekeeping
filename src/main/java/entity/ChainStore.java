package entity;

public class ChainStore {
	private int chainId ;
	private int clientId;
	private String chainName;
	private int status;
	
	
	public int getClientId() {
		return clientId;
	}


	public void setClientId(int clientId) {
		this.clientId = clientId;
	}


	public int getChainId() {
		return chainId;
	}


	public void setChainId(int chainId) {
		this.chainId = chainId;
	}


	public String getChainName() {
		return chainName;
	}


	public void setChainName(String chainName) {
		this.chainName = chainName;
	}


	public int getStatus() {
		return status;
	}


	public void setStatus(int status) {
		this.status = status;
	}


	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public String toString(){
		return clientId + "," + chainName;
	}

}
