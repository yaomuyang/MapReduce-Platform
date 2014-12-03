package info;

import java.io.Serializable;

/*
 * This class contains the job's status, whether it is finished, failed, running.....
 */
@SuppressWarnings("serial")
public class JobStatus implements Serializable{
	private JobId id;
	private int status;
	
	public static final int NEW = 1;
	public static final int RUNNING = 2;
	public static final int SUCCEEDED = 3;
	public static final int FAILED = 4;
	
	//upon creating a job status, we intuitively set the status to NEW
	public JobStatus(JobId id){
		this.id = id;
		this.status = JobStatus.NEW;
	}
	
	public JobId getId(){
		return this.id;
	}
	
	public void setStatus(int status){
		this.status = status;
	}
	
	public int getStatus(){
		return this.status;
	}
}