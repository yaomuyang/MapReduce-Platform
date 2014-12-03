package info;

import java.io.Serializable;

/*
 * This class contains the configuration of a Mapper/Reducer
 * This is generated in the application manager of a mapreduce job, and is given to the resourcemanager to schedule
 * The NodeManager should typically be able to start a new Mapper/Reducer using the information stored in this class
 */
@SuppressWarnings("serial")
public class JobConf implements Serializable{
	private JobId id;
	
	JobConf(){
		this.id = new JobId();
	}
	
	public void setId(int id){
		this.id.id = id;
	}
	
	public JobId getId(){
		return this.id;
	}
	
	public void setApplicationId(int id){
		this.id.applicationId.id = id;
	}
	
	public boolean equals(Object obj){
		JobConf jobConf = (JobConf)obj;
		if(this.id.equals(jobConf.id)){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode(){
		return this.id.id;
	}
}