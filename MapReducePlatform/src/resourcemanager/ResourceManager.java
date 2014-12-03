package resourcemanager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import commons.Communication;
import commons.Configuration;
import dfs.DFSClient;
import dfs.info.DFSFileInfo;
import info.ApplicationInfo;
import info.ApplicationSubmissionInfo;
import info.JobConf;
import info.MapperInfo;
import info.NodeStatus;
import info.ReducerInfo;
import mapreduce.Output;

/*
 * The core controller of our "Hadoop", where we receive applications and manage all the mappers and reducerss
 */
public class ResourceManager {
	//configuration generated from the config file
	Configuration conf;
	
	Scheduler scheduler;
	
	ApplicationManager applicationManager;
	
	ServerSocket ss;

	public static void main(String[] args){
		try{
			ResourceManager resourceManager = new ResourceManager();
			resourceManager.run();
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
		
	ResourceManager() throws Exception{
		this.conf = new Configuration();
		System.out.println("[ResourceManager]: Configuration set up");
		
		this.scheduler = new Scheduler();
		this.scheduler.initiate(this.conf);
		System.out.println("[ResourceManager]: Scheduler initiated");
		
		this.applicationManager = new ApplicationManager();
		this.applicationManager.initiate(conf);
		System.out.println("[ResourceManager]: ApplicationManager initiated");
		
		ss = new ServerSocket(Integer.parseInt(conf.getProperty("ResourceManagerHeartBeatPort")));
	}
		
	public void run() throws Exception{
			
		//start the thread to listen for new commands from client
		HandleApplicationRunnable handleApplicationRunnable = new HandleApplicationRunnable();
		Thread monitorNewApplicationThread = new Thread(handleApplicationRunnable);
		monitorNewApplicationThread.start();
		
		//use serversocket to receive heartbeat update from the nodemanager
		while(true){
			//Accept the heartbeat status report
			Socket s = ss.accept();
			Thread monitorNodeStatusThread = new Thread(new MonitorNodeStatusRunnable(s));
			monitorNodeStatusThread.start();
		}	
	}
		
	/*
	 * This is a thread that listens to the heartbeat status report from the nodemanager
	 * Also, this node is responsible for the registration of new nodemanager
	 */
	class MonitorNodeStatusRunnable implements Runnable{
		Socket s;
		MonitorNodeStatusRunnable(Socket s){
			this.s = s;
		}
		
		public void run(){
			try{				
				NodeStatus node = (NodeStatus)Communication.receiveObject(s);
				System.out.println("[ResourceManager - MonitorNodeStatus]: HeartBeat report from node: " + node.getId().id);
				
				JobsSchedulingInfo jobsSchedulingInfo = applicationManager.updateJobs(node);
				
				//update the node status and schedule new jobs
				scheduler.updateJobs(jobsSchedulingInfo);
				scheduler.updateNodeStatus(node);
				Communication.sendString(""+node.getId().id, s);
				ArrayList<JobConf> newJobs = scheduler.schedule(node.getId());
					
				//send the new jobs back
				Communication.sendObject(newJobs, s);
				if(newJobs.size()!=0){
					System.out.println("[ResourceManager - MonitorNodeStatus]: New jobs sent to node "+ node.getId().id);
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
		
	/* 
	 * this thread performs the following functions
	 * 1. received the submitted user applications from the handler on the client side. 
	 * 2. Operates on the configuration parameters below to determine the chunk of the file each mapper should
	 * 	  use ( denoted by offset )
	 * 		i. RECORDSIZE - The length of the fixed length record
	 * 		ii. MAPSIZE - the number of records each mapper can handle	
	 */
	class HandleApplicationRunnable implements Runnable{
		int portNumber;
		ServerSocket serverSocket;
		int reducerCount;
		
		public HandleApplicationRunnable(){
			
			this.portNumber = Integer.parseInt(conf.getProperty("ResourceManagerNewAppPort"));
			this.reducerCount = Integer.parseInt(conf.getProperty("ReducerCount"));
			try{
				serverSocket = new ServerSocket(portNumber);
			}catch(Exception ioException){
				ioException.printStackTrace();
			}
		}

		/* 
		 * the run method has the following functions:
		 * 1. Listen on some portNumber for user submitted applications.
		 * 2. Read the application information in the form of JobInfo object
		 * 3. determine the recordsize and mapsize parameters from the configuration file
		 * 4. invoke MapReduce method to construct mapper and reducer info
		 */
		@Override
		public void run(){
		
			while(true){				
				try{
					Socket clientSocket = serverSocket.accept();					
					System.out.println("[ResourceManager - HandleApplication]: Socket connection established, new application comming in");
					
					//receive the application's information
					ApplicationSubmissionInfo application = (ApplicationSubmissionInfo)Communication.receiveObject(clientSocket);
					//get the fileinfo from DFS
					DFSFileInfo file = (new DFSClient(conf)).getFileInfo(application.getInputFile());
					//set the configuration of the application
					ApplicationInfo applicationInfo = packMapReduce(application, file);
					System.out.println("[ResourceManager - HandleApplication]: Application packed into mappers and reducers");
					
					JobsSchedulingInfo jobsSchedulingInfo = applicationManager.addApplication(applicationInfo);
					System.out.println("[ResourceManager - HandleApplication]: New Jobs submitted to scheduler");
					scheduler.updateJobs(jobsSchedulingInfo);
					
				}catch(Exception e){
					e.printStackTrace();
					System.exit(1);
				}
			}			
		}
		
		private ApplicationInfo packMapReduce(ApplicationSubmissionInfo application, DFSFileInfo file) throws Exception{	
			ApplicationInfo applicationInfo = new ApplicationInfo();
			this.setMappers(application, applicationInfo, file);			
			this.setReducers(application, applicationInfo, file);
			return applicationInfo;		
		}
		
		private void setMappers(ApplicationSubmissionInfo application, ApplicationInfo applicationInfo, DFSFileInfo file) throws Exception{

			//set the number of mappers according to the number of chunks in the input file
			application.setMapperCount(file.getChunks().size());
			System.out.println("[ResourceManager - HandleApplication]: mapperCount = "+application.getMapperCount());
				
			/* 
			 * construct a list of mapperInfo objects. Each mapperInfo object represents the information required by
			 * a mapper of the application to run.
			 */
			ArrayList<MapperInfo> mapperList = new ArrayList<MapperInfo>();
			for(int count=0;count<application.getMapperCount();count++){
				MapperInfo mapperInfo = new MapperInfo(file.getChunks().get(count), application.getMapClass());
				//just arbitrarily set the chunk to a random one, not caring about the locality of data
				mapperInfo.setChunk(file.getChunks().get(count));
				mapperList.add(mapperInfo);
			}				
			applicationInfo.setMappers(mapperList);
		}
		
		private void setReducers(ApplicationSubmissionInfo application, ApplicationInfo applicationInfo, DFSFileInfo file){
			application.setReducerCount(reducerCount);
			System.out.println("[ResourceManager - HandleApplication]: reducerCount = "+application.getReducerCount());
			
			ArrayList<ReducerInfo> reducerList = new ArrayList<ReducerInfo>();
			for(int count=0; count<application.getReducerCount(); count++){
				ArrayList<String> reducerOutputFileList = new ArrayList<String>();
				reducerOutputFileList.add(application.getOutputFile()+"."+count);
				Output output = new Output(reducerOutputFileList);
				ReducerInfo reducerInfo = new ReducerInfo(output, application.getReduceClass());
				reducerList.add(reducerInfo);
			}
			applicationInfo.setReducers(reducerList);
		}
	}
}
