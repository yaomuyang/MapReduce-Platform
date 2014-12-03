package info;

import java.io.Serializable;

/* 
 * The main structure used for information flow between the point where the user submits an application
 * to run on our map-reduce framework and the HandleApplication process on ResourceManager at the master node 
 */
@SuppressWarnings("serial")
public class ApplicationSubmissionInfo implements Serializable{
	String jobName;
	Class<?> mapClass;
	Class<?> reduceClass;
	String inputFile;
	String outputFile;
	int reducerCount;
	int mapperCount;
	
	public ApplicationSubmissionInfo(String jobName){
		this.jobName = jobName;
	}
	
	public void setMapClass(Class<?> c){
		this.mapClass = c;
	}
	
	public void setReduceClass(Class<?> c){
		this.reduceClass = c;
	}
	
	public Class<?> getMapClass(){
		return(this.mapClass);
	}
	
	public Class<?> getReduceClass(){
		return(this.reduceClass);
	}

	public void setInputFile(String inputFile){
		this.inputFile = inputFile;
	}
	
	public String getInputFile(){
		return(this.inputFile);
	}

	public void setOutputFile(String outputFile){
		this.outputFile = outputFile;
	}
	
	public String getOutputFile(){
		return(this.outputFile);
	}

	public void setMapperCount(int mapperCount){
		this.mapperCount = mapperCount;
	}
	
	public int getMapperCount(){
		return this.mapperCount;
	}
	
	public void setReducerCount(int reducerCount){
		this.reducerCount = reducerCount;
	}
	
	public int getReducerCount(){
		return this.reducerCount;
	}
}