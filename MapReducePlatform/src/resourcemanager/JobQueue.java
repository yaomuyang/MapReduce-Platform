package resourcemanager;

import java.util.LinkedList;
import java.util.List;
import info.JobConf;

public class JobQueue {
	//the queue that maintains the list jobs, this is 
	private static LinkedList<JobConf> jobs = new LinkedList<JobConf>();
	
	//put new jobs into the jobqueue, This should envolve more complicated logic, but I just make it simple for now
	public void addJob(List<JobConf> newJobs){
		if(newJobs.size()!=0)
			System.out.println("[ResourceManager - Scheduler]: "+newJobs.size()+" New job put into the queue");
		for(JobConf job : newJobs){
			jobs.addLast(job);
		}
	}
	
	//add prioritized job to the jobqueue
	public void addPrioritizedJob(List<JobConf> newJobs){
		if(newJobs.size()!=0){
			System.out.println("[ResourceManager - Scheduler]: "+newJobs.size()+" New job put into the prioritized queue");
		}
		for(JobConf job : newJobs){
			jobs.addFirst(job);
		}
	}
	
	public void removeJobs(List<JobConf> jobConfs){
		jobs.removeAll(jobConfs);
	}
	
	//return the job with highest priority, for now, we just get the first element in the queue 
	public JobConf getJob(){
		return jobs.removeFirst();
	}
	
	public boolean isEmpty(){
		return jobs.isEmpty();
	}
}
