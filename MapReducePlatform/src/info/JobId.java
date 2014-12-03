package info;

import java.io.Serializable;

/*
 * This class is typically a job's id number, which is unique and could be used to distinguish the job
 */
@SuppressWarnings("serial")
public class JobId implements Serializable{
	public int id;
	public ApplicationId applicationId = new ApplicationId();
	
	public boolean equals(Object obj){
		JobId jobId = (JobId)obj;
		if(this.id == jobId.id){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode(){
		return this.id;
	}
}