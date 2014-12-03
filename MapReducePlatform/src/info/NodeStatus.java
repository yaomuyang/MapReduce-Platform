package info;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

/*
 * This contains the status of a node, with the basic info "Node", as well as an array of jobs in that node
 * This is the class that nodemanager sends to the resourcemanger in the status report
 */
@SuppressWarnings("serial")
public class NodeStatus extends Node implements Serializable{
	private ArrayList<JobStatus> jobs;
	
	public NodeStatus(InetAddress address, int maximumJobs){
		super(address, maximumJobs);
		this.jobs = new ArrayList<JobStatus>();		
	}
	
	public ArrayList<JobStatus> getJobs(){
		return this.jobs;
	}
	
	public void setJobs(Collection<JobStatus> jobs){
		this.jobs.addAll(jobs);
	}
}