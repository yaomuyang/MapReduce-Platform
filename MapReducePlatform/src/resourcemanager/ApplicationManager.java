package resourcemanager;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import commons.Configuration;
import info.ApplicationId;
import info.ApplicationInfo;
import info.JobConf;
import info.JobId;
import info.JobStatus;
import info.MapperInfo;
import info.NodeStatus;
import info.ReducerInfo;
import info.ReducerInput;
import mapreduce.Output;

/*
 * This manager resides in the resourcemanager to manage the application status, as well as all the job status
 */
public class ApplicationManager {
	
	private static int jobReattemptTimes;
	
	private static int incrementalApplicationId;
	
	private static int incrementalJobId;
	
	private HashMap<JobId, ApplicationId> job2AppMap;
	private HashMap<ApplicationId, ApplicationInfo> applications;
	private HashMap<JobId, Integer> failedJobs;
	
	public void initiate(Configuration conf){
		ApplicationManager.jobReattemptTimes = Integer.parseInt(conf.getProperty("JobReattemptTimes"));
		ApplicationManager.incrementalApplicationId = 1;
		ApplicationManager.incrementalJobId = 1;
		this.job2AppMap = new HashMap<JobId, ApplicationId>();
		this.applications = new HashMap<ApplicationId, ApplicationInfo>();	
		this.failedJobs = new HashMap<JobId, Integer>();
	}
	
	//This function add a new application, then return a list of jobs that we can put to schedule
	public JobsSchedulingInfo addApplication(ApplicationInfo applicationInfo){
		applicationInfo.setApplicationId(incrementalApplicationId);
		incrementalApplicationId = incrementalApplicationId + 1;
		
		this.applications.put(applicationInfo.getApplicationId(), applicationInfo);
		
		System.out.println("[ResourceManager - ApplicationManager]: New Application submitted, with application id: " + applicationInfo.getApplicationId().id);
		
		//add all the mappers in to the scheduling queue
		JobsSchedulingInfo jobsSchedulingInfo = new JobsSchedulingInfo();
		jobsSchedulingInfo.newJobs.addAll(packMappers(applicationInfo));
		return jobsSchedulingInfo;
	}
	
	private void abortApplication(ApplicationId applicationId, JobsSchedulingInfo jobsSchedulingInfo){
		System.out.println("[ResourceManager - ApplicationManager]: Application Aborted, with application id: " + applicationId.id);
		ArrayList<JobConf> jobConfs = applications.get(applicationId).getAllJobs();
		jobsSchedulingInfo.canceledJobs.addAll(jobConfs);
		for(JobConf jobConf: jobConfs){
			job2AppMap.remove(jobConf.getId());
			failedJobs.remove(jobConf.getId());
		}
		applications.remove(applicationId);
	}
	
	//update jobs, return a list of prioritized jobs, which are typically previous failed jobs or reducers
	public JobsSchedulingInfo updateJobs(NodeStatus node){
		JobsSchedulingInfo jobsSchedulingInfo = new JobsSchedulingInfo();
		for(JobStatus job: node.getJobs()){
			//if the job is already finished or failed, skip this
			if(!applications.containsKey(job.getId().applicationId))
				continue;
			
			//else, check the status and act accordingly
			if(job.getStatus()==JobStatus.SUCCEEDED){
				ApplicationInfo application = applications.get(this.job2AppMap.get(job.getId()));
				//if this is a mapper
				if(application.getJob(job.getId()).getClass().equals(MapperInfo.class)){
					application.setMapperFinished(job.getId(), node.getInetAddress());
					if(application.isMappersFinished()==true){
						System.out.println("[ResourceManager - ApplicationManager]: All mappers finished on application: " + job.getId().applicationId.id);
						//add the reducers into the queueu
						jobsSchedulingInfo.prioritizedNewJobs.addAll(this.packReducers(application));
					}
				}
			}else if(job.getStatus()==JobStatus.FAILED){
				//count the failed times of the current job
				int failedTimes = 1;
				if(failedJobs.containsKey(job.getId())){
					failedTimes = failedJobs.get(job.getId()) + 1;
				}
				//if the failed time is too much, we can do nothing but abort the whole application
				if(failedTimes>=ApplicationManager.jobReattemptTimes){
					abortApplication(job.getId().applicationId, jobsSchedulingInfo);
				}else{
					failedJobs.put(job.getId(), failedTimes);
					jobsSchedulingInfo.prioritizedNewJobs.add(applications.get(job.getId().applicationId).getJob(job.getId()));
				}
			}
		}
		return jobsSchedulingInfo;
	}
	
	//pack the mappers into jobconf, well at the same time assign their job id
	private ArrayList<MapperInfo> packMappers(ApplicationInfo applicationInfo){
		ArrayList<MapperInfo> newMappers = new ArrayList<MapperInfo>();		
		newMappers.addAll(applicationInfo.getNewMappers());
				
		//assign job ids
		for(MapperInfo mapper : newMappers){
			mapper.setId(incrementalJobId);
			mapper.setApplicationId(applicationInfo.getApplicationId().id);
			incrementalJobId = incrementalJobId + 1;
			//set the intermidiate filenames and output
			ArrayList<String> intermediateFiles = new ArrayList<String>();
			for(int fileCount = 0; fileCount < applicationInfo.getNewReducers().size() ; fileCount++)	{
				intermediateFiles.add("Application"+applicationInfo.getApplicationId().id+".Job"+mapper.getId().id+".im"+fileCount);
			}
			mapper.setOutput(new Output(intermediateFiles));
			//put the mappers into local data structure
			this.job2AppMap.put(mapper.getId(), applicationInfo.getApplicationId());
		}
		
		applicationInfo.pushMappersToHashMap();
		return newMappers;
	}
	
	private ArrayList<ReducerInfo> packReducers(ApplicationInfo applicationInfo){
		ArrayList<ReducerInfo> newReducers = new ArrayList<ReducerInfo>();
		newReducers.addAll(applicationInfo.getNewReducers());
		
		for(int reducerCount=0; reducerCount<newReducers.size(); reducerCount++){
			//get the reducer object and set job id
			ReducerInfo reducer = newReducers.get(reducerCount);
			reducer.setId(incrementalJobId);
			reducer.setApplicationId(applicationInfo.getApplicationId().id);
			incrementalJobId = incrementalJobId + 1;
			//set the intermediate files according to the finished mappers
			ArrayList<ReducerInput> reducerInputList = new ArrayList<ReducerInput>();
			Iterator<Entry<JobId, InetAddress>> it = applicationInfo.getFinishedMappers().entrySet().iterator();
			while(it.hasNext()){
				Entry<JobId, InetAddress> entry = (Entry<JobId, InetAddress>)it.next();
				String fileName = ((MapperInfo)applicationInfo.getJob(entry.getKey())).getOutput().getFileName()[reducerCount];
				reducerInputList.add(new ReducerInput(fileName, entry.getValue()));
			}
			reducer.setReducerInputList(reducerInputList);
			//put it into the job2app map
			this.job2AppMap.put(reducer.getId(), applicationInfo.getApplicationId());
			System.out.println("Debug: "+reducer.getReducerClass());
		}
		
		applicationInfo.pushReducersToHashMap();
		return newReducers;
	}
}