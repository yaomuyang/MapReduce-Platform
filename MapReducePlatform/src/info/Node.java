package info;

import java.io.Serializable;
import java.net.InetAddress;

/*
 * This class stores the basic information of a node, including of its address and how many jobs it can contain
 */
@SuppressWarnings("serial")
public class Node implements Serializable {
	private NodeId id;
	private InetAddress address;
	private int maximumJobs;
	
	public Node(InetAddress address, int maximumJobs){
		this.address = address;
		this.maximumJobs = maximumJobs;
		this.id = new NodeId();
	}	
	
	public Node(Node node){
		this(node.getInetAddress(), node.getMaximumJobs());
		this.id = node.getId();
	}
	
	public void setId(int id){
		this.id.id = id;
	}
	
	public NodeId getId(){
		return this.id;
	}
	
	public InetAddress getInetAddress(){
		return this.address;
	}
	
	public int getMaximumJobs(){
		return this.maximumJobs;
	}
}