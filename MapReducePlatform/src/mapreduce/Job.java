package mapreduce;

import java.net.Socket;
import commons.Communication;
import info.ApplicationSubmissionInfo;

/* 
 * this is a component that runs on the user side. Any submitted application is captured by this client
 * and send to the HandleApplication component of the Resource Manager at the master node. 
 * This enables user job submission to be distributed.
 */
public class Job {

	/* JobInfo which will encapsulate all information about the submitted application */
	private ApplicationSubmissionInfo application;
	
	public Job(String jobName){
		this.application = new ApplicationSubmissionInfo(jobName);
	}
	
	public void setMapClass(Class<?> c){
		this.application.setMapClass(c);
	}
	
	public void setReduceClass(Class<?> c){
		this.application.setReduceClass(c);
	}
	
	public void setInputFile(String inputFile){
		this.application.setInputFile(inputFile);
	}
	
	public void setOutputFile(String outputFile){
		this.application.setOutputFile(outputFile);
	}
		
	/* create a socket connection to the HandlleApplication component of the Resource Manager on the master */
	public void runJob(String[] args) throws Exception{			
		System.out.println("[ApplicationClient]: connecting to resourcemanager");
		Socket clientSocket = new Socket(args[0], Integer.parseInt(args[1]));

		/* send the application to the master node */
		Communication.sendObject(this.application, clientSocket);
			
		System.out.println("[ApplicationClient]: application submitted succesfully");
	}
}