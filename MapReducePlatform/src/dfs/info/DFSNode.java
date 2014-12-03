package dfs.info;

import java.io.Serializable;
import java.net.InetAddress;

@SuppressWarnings("serial")
public class DFSNode implements Serializable{
	private DFSNodeId id;
	private InetAddress address;
	
	public DFSNode(InetAddress address){
		this.address = address;
		this.id = new DFSNodeId();
	}	
	
	public DFSNode(DFSNode node){
		this(node.getInetAddress());
		this.id = node.getId();
	}
	
	public void setId(int id){
		this.id.id = id;
	}
	
	public DFSNodeId getId(){
		return this.id;
	}
	
	public InetAddress getInetAddress(){
		return this.address;
	}
	
	public void setAddress(InetAddress address){
		this.address = address;
	}
	
	public boolean equals(Object obj){
		DFSNode node = (DFSNode)obj;
		if(this.id.equals(node.getId())){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode(){
		return this.id.id;
	}
}