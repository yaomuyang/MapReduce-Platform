package nodemanager;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;
import dfs.DFSClient;
import info.JobConf;
import info.JobId;
import info.JobStatus;
import info.MapperInfo;
import info.NodeStatus;
import info.ReducerInfo;
import commons.Communication;
import commons.Configuration;

public class NodeManager {
	
	//configuration generated from the config file
	private Configuration conf;
	
	private ServerSocket ss;
	
	private HashMap<JobId, JobStatus> jobStatus;
	private HashMap<JobId, ThreadInfo> jobThreadInfos;
	private HashMap<JobId, String> reducerOutputs;

	//address of the ResourceManager
	private InetSocketAddress rmAddress;
	
	private int nodeId = 0;
	
	public static void main(String[] args){
		try{
			NodeManager nodeManager = new NodeManager();
			nodeManager.run();
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	NodeManager() throws Exception{
		this.conf = new Configuration();
		this.rmAddress = new InetSocketAddress(conf.getProperty("ResourceManagerIP"), Integer.parseInt(conf.getProperty("ResourceManagerHeartBeatPort")));
		this.jobStatus = new HashMap<JobId, JobStatus>();
		this.jobThreadInfos = new HashMap<JobId, ThreadInfo>();
		this.reducerOutputs = new HashMap<JobId, String>();
		this.ss = new ServerSocket(Integer.parseInt(conf.getProperty("NodeManagerPort")));
		//creating folder for intermediate files
		File file = new File("IM");
		file.mkdir();
	}
	
	public void run() throws Exception{
		
		this.registerToRM();
		
		//start the thread to monitor running jobs, send heartbeat status report to the resourcemanager
		MonitorRunningJobRunnable monitorRunningJobRunnable = new MonitorRunningJobRunnable();
		Thread monitorRunningJobThread = new Thread(monitorRunningJobRunnable);
		monitorRunningJobThread.start();
		
		while(true){
			Socket s = ss.accept();
			Thread tempThread = new Thread(new NodeManagerSocketRunnable(s));
			tempThread.start();
		}
	}
	
	/*
	 * This function registers to the ResourceManager, report its status, and add itself to the scheduler
	 */
	private void registerToRM() throws Exception{
		//create an empty node status
		NodeStatus node = new NodeStatus(InetAddress.getLocalHost(), Integer.parseInt(conf.getProperty("DefaultNodeCapacity")));
		//send message and recevie new jobs
		ArrayList<JobConf> newJobs = this.sendHeartBeatMessage(node);
		System.out.println("[NodeManager] NodeManager istablished, registered to resourcemanager");
		this.runJobs(newJobs);
	}
	
	private void runJobs(ArrayList<JobConf> newJobs) throws Exception{
		for(JobConf job: newJobs){	
			//Add the jobs into the HashMap of the current jobs
			jobStatus.put(job.getId(), new JobStatus(job.getId()));
			
			//start the job, set the job status accordingly
			if(runJob(job)==true){
				jobStatus.get(job.getId()).setStatus(JobStatus.RUNNING);
				System.out.println("[NodeManager] New Job: " + job.getId().id + " is running");
			}else{
				jobStatus.get(job.getId()).setStatus(JobStatus.FAILED);
				System.out.println("[NodeManager] New Job: " + job.getId().id + " failed to start running");
			}
		}
	}
	
	/*
	 * run the job using jobconf, return if the job is running succesfully
	 */
	private boolean runJob(JobConf job) throws Exception{
		if(job.getClass().equals(MapperInfo.class)){
			return this.runMapper((MapperInfo)job);
		}else if(job.getClass().equals(ReducerInfo.class)){
			return this.runReducer((ReducerInfo)job);
		}else{
			return false;
		}
	}
	
	private boolean runMapper(MapperInfo mapperInfo) throws Exception{
		System.out.println("[NodeManager] Starting a new Mapper with Jobid : " + mapperInfo.getId().id);
		ThreadInfo threadInfo = new ThreadInfo();
		Thread mapperThread = new Thread(new MapperRunnable(mapperInfo, threadInfo.callback, this.conf));
		mapperThread.setUncaughtExceptionHandler(threadInfo.exceptionHandler);
		threadInfo.thread = mapperThread;
		mapperThread.start();
		jobThreadInfos.put(mapperInfo.getId(), threadInfo);
		if(!mapperThread.isAlive()){
			return false;
		}else{
			return true;
		}
	}
	
	private boolean runReducer(ReducerInfo reducerInfo){
		System.out.println("[NodeManager] Starting a new Mapper with Jobid : " + reducerInfo.getId().id);
		ThreadInfo threadInfo = new ThreadInfo();
		Thread reducerThread = new Thread(new ReducerRunnable(reducerInfo, threadInfo.callback, this.conf));
		threadInfo.thread = reducerThread;
		reducerThread.start();
		jobThreadInfos.put(reducerInfo.getId(), threadInfo);
		this.reducerOutputs.put(reducerInfo.getId(), reducerInfo.getOutput().getFileName()[0]);
		if(!reducerThread.isAlive()){
			return false;
		}else{
			return true;
		}
	}
	
	/*
	 * This is a thread that monitors the jobs running, and react upon job finish or job failed
	 * Also, this thread is responsible for turning in heatbeat status report to the resourcemanager
	 */
	class MonitorRunningJobRunnable implements Runnable{
		public void run(){
			try{
				while(true){
					Thread.sleep(10000);
					System.out.println("[NodeManager] Sending heartbeat report to ResourceManager");
					for(JobId jobId: jobStatus.keySet()){
						updateStatus(jobId);
					}
					NodeStatus node = new NodeStatus(InetAddress.getLocalHost(), Integer.parseInt(conf.getProperty("DefaultNodeCapacity")));
					node.setJobs(jobStatus.values());
					ArrayList<JobConf> newJobs = sendHeartBeatMessage(node);
					cleanStatus();
					runJobs(newJobs);
				}
			}catch(Exception e){
				e.printStackTrace();
				System.exit(1);
			}
		}        
		
		private void updateStatus(JobId jobId) throws Exception{
			ThreadInfo jobThreadInfo = jobThreadInfos.get(jobId);
			if(jobThreadInfo.thread.getState()==Thread.State.TERMINATED){
				if(jobThreadInfo.failed==true){
					jobStatus.get(jobId).setStatus(JobStatus.FAILED);
					System.out.println("*******************************[NodeManager] Job "+jobId.id+" failed");
				}else{
					System.out.println("[NodeManager] Job : " + jobId.id + " successfully finished");
					jobStatus.get(jobId).setStatus(JobStatus.SUCCEEDED);
					if(reducerOutputs.containsKey(jobId)){
						DFSClient dfsClient = new DFSClient(conf);
						dfsClient.uploadFile("IM/"+reducerOutputs.get(jobId));
						reducerOutputs.remove(jobId);
					}
				}
			}
		}
		
		private void cleanStatus(){
			Iterator<Entry<JobId, JobStatus>> it = jobStatus.entrySet().iterator();
			while(it.hasNext()){
				Entry<JobId, JobStatus> item = (Entry<JobId, JobStatus>)it.next();
				if(item.getValue().getStatus()!=JobStatus.RUNNING){
					jobThreadInfos.remove(item.getKey());
					it.remove();					
				}
			} 
		}
	}
	
	/*
	 * send heartbeat report, and receive new jobs
	 */
	private ArrayList<JobConf> sendHeartBeatMessage(NodeStatus node) throws Exception{
		node.setId(nodeId);
		
		Socket s = new Socket(this.rmAddress.getAddress(), this.rmAddress.getPort());
		
		//send the status report
		Communication.sendObject(node, s);
		
		//receive node id
		this.nodeId = Integer.parseInt(Communication.receiveString(s));
		
		//receive new jobs
		@SuppressWarnings("unchecked")
		ArrayList<JobConf> newJobs = (ArrayList<JobConf>)Communication.receiveObject(s);
		
		s.close();
		return newJobs;
	}
	
	/*
	 * This socket deal with the case when reducer asks for intermediate files here
	 */
	class NodeManagerSocketRunnable implements Runnable{
		Socket s;
		NodeManagerSocketRunnable(Socket s){
			this.s = s;
		}	
		public void run(){
			try{
				String fileName = "IM/" + Communication.receiveString(s);
					
				//read in the file
				File intermediateFile = new File(fileName);
				byte[] bytes = new byte[(int)intermediateFile.length()];
				FileInputStream fis = new FileInputStream(intermediateFile);
				BufferedInputStream bis = new BufferedInputStream(fis);
				bis.read(bytes, 0, bytes.length);
				bis.close();
				fis.close();
				
				//send the intermediate file over to the reducer
				//System.out.println("[NodeManager]: Sending intermediate file, filename = "+intermediateFile.getName());
				Communication.sendBytes(s, bytes);
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}