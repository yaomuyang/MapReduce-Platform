package resourcemanager;

import java.util.ArrayList;
import java.util.HashMap;
import info.JobConf;
import info.NodeStatus;
import info.NodeId;
import commons.Configuration;

/*
 * The major scheduler class, this resides in resourcemanager as the overall resource control
 */
public class Scheduler {
	
	private static HashMap<NodeId, NodeScheduleInfo> nodes;
	private static JobQueue jobQueue;
	
	private static int incrementalNodeId;
	
	public void updateNodeStatus(NodeStatus nodeStatus){
		//register the node if it is a new one
		if(!nodes.keySet().contains(nodeStatus.getId())){
			this.registerNode(nodeStatus);
		}else{
			NodeScheduleInfo node = Scheduler.nodes.get(nodeStatus.getId());
			node.UpdateNodeInfo(nodeStatus);
		}
	}
	
	public void initiate(Configuration conf){
		Scheduler.incrementalNodeId = 1;
		Scheduler.nodes = new HashMap<NodeId, NodeScheduleInfo>();
		Scheduler.jobQueue = new JobQueue();
	}
	
	//this function registers a new node into the cluster
	private void registerNode(NodeStatus nodeStatus){
		//set the node id
		nodeStatus.setId(incrementalNodeId);
		incrementalNodeId = incrementalNodeId + 1;
		//create new ScheduleInfo, and put it into the HashMap
		NodeScheduleInfo node = new NodeScheduleInfo(nodeStatus);
		nodes.put(nodeStatus.getId(), node);
		System.out.println("[ResourceManager - Scheduler]: New NodeManager registered, with NodeId = "+node.getId().id);
	}
	
	//This function is the core of the Scheduler, it schedule jobs 
	//and enforce the resourcemanager to tell the nodemanagers to start the job
	public ArrayList<JobConf> schedule(NodeId nodeId){
		System.out.println("[ResourceManager - Scheduler]: Scheduling Job on Node: "+nodeId.id);
		//interate through all the nodes to find vacancy
		NodeScheduleInfo node = Scheduler.nodes.get(nodeId);
		ArrayList<JobConf> newJobs = new ArrayList<JobConf>();
		for(int i=0; i<node.getFreeSpace()&&(!Scheduler.jobQueue.isEmpty()); i++){
			JobConf newJob = Scheduler.jobQueue.getJob();
			newJobs.add(newJob);
			System.out.println("[ResourceManager - Scheduler]: New job scheduled on node: " + nodeId.id + ", with JobId = "+newJob.getId().id);
		}
		node.assignNewJobs(newJobs);
		return newJobs;
	}
	
	public void updateJobs(JobsSchedulingInfo jobsSchedulingInfo){
		Scheduler.jobQueue.addJob(jobsSchedulingInfo.newJobs);
		Scheduler.jobQueue.addPrioritizedJob(jobsSchedulingInfo.prioritizedNewJobs);
		Scheduler.jobQueue.removeJobs(jobsSchedulingInfo.canceledJobs);
	}
}
