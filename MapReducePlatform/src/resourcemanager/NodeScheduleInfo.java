package resourcemanager;

import java.util.ArrayList;
import java.util.HashMap;
import info.Node;
import info.NodeStatus;
import info.JobId;
import info.JobConf;
import info.JobStatus;

/*
 * This contains the detailed information of a node, including its running jobs, as well as the detailed JobInfo for recovery
 */
@SuppressWarnings("serial")
public class NodeScheduleInfo extends Node{
	
	HashMap<JobId, JobStatus> jobStatus;
	
	public NodeScheduleInfo(Node node){
		super(node);
		this.jobStatus = new HashMap<JobId, JobStatus>();
	}
	
	//update node info, return a bunch of jobs that we need to reschedule and do again
	public void UpdateNodeInfo(NodeStatus node){
		ArrayList<JobStatus> jobs = node.getJobs();
		for(JobStatus job : jobs){
			JobId jobId = job.getId();
			if(job.getStatus()==JobStatus.FAILED){
				jobStatus.remove(jobId);
				System.out.println("[ResourceManager - Scheduler]: In node "+node.getId().id+", Job "+jobId.id+" has failed");
			}else if(job.getStatus()==JobStatus.SUCCEEDED){
				//if the job succeeded, I just remove it from the node
				jobStatus.remove(jobId);
				System.out.println("[ResourceManager - Scheduler]: In node "+node.getId().id+", Job "+jobId.id+" has succeeded");
			}else{
				//if the job status is elsewise, I just update the status, doesn't bother
				jobStatus.get(jobId).setStatus(job.getStatus());
			}
		}
	}
	
	//get the amount of extra jobs that a certain node can hold
	public int getFreeSpace(){
		return this.getMaximumJobs()-this.jobStatus.size();
	}
	
	public void assignNewJobs(ArrayList<JobConf> jobs){
		for(JobConf job:jobs){
			this.jobStatus.put(job.getId(), new JobStatus(job.getId()));
		}
	}
}
