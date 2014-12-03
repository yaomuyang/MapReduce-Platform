package info;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;

/*
 * This contains the information of a mapreduce application 
 * This is created by the application dispatcher, and is organized and managed in the resourcemanager
 */
public class ApplicationInfo {

	ApplicationId id = new ApplicationId();
	HashMap<JobId, MapperInfo> mapperInfos = new HashMap<JobId, MapperInfo>();
	HashMap<JobId, ReducerInfo> reducerInfos = new HashMap<JobId, ReducerInfo>();
	ArrayList<MapperInfo> newMappers = new ArrayList<MapperInfo>();
	ArrayList<ReducerInfo> newReducers = new ArrayList<ReducerInfo>();
	HashMap<JobId, InetAddress> finishedMappers = new HashMap<JobId, InetAddress>();
	
	public void setApplicationId(int applicationId){
		this.id.id = applicationId;
	}
	
	public ApplicationId getApplicationId(){
		return this.id;
	}
	
	public void setMappers(ArrayList<MapperInfo> mapperInfoList){
		this.newMappers.addAll(mapperInfoList);
	}
	
	public void setReducers(ArrayList<ReducerInfo> reducerInfos){
		this.newReducers.addAll(reducerInfos);
	}
	
	public ArrayList<MapperInfo> getNewMappers(){
		return this.newMappers;
	}
	
	public ArrayList<ReducerInfo> getNewReducers(){
		return this.newReducers;
	}
	
	public JobConf getJob(JobId jobId){
		if(reducerInfos.containsKey(jobId)){
			return reducerInfos.get(jobId);
		}else if(mapperInfos.containsKey(jobId)){
			return mapperInfos.get(jobId);
		}else{
			return null;
		}
	}
	
	public ArrayList<JobConf> getAllJobs(){
		ArrayList<JobConf> jobConfs = new ArrayList<JobConf>();
		jobConfs.addAll(reducerInfos.values());
		jobConfs.addAll(mapperInfos.values());
		return jobConfs;
	}
	
	public void pushMappersToHashMap(){
		for(MapperInfo mapper: this.newMappers){
			this.mapperInfos.put(mapper.getId(), mapper);
		}
		this.newMappers.clear();
	}
	
	public void pushReducersToHashMap(){
		for(ReducerInfo reducer: this.newReducers){
			this.reducerInfos.put(reducer.getId(), reducer);
		}
		this.newReducers.clear();
	}
	
	public void setMapperFinished(JobId jobId, InetAddress address){
		this.finishedMappers.put(jobId, address);
	}
	
	public boolean isMappersFinished(){
		if(this.finishedMappers.size()==this.mapperInfos.size()){
			return true;
		}else{
			return false;
		}
	}
	
	public HashMap<JobId, InetAddress> getFinishedMappers(){
		return this.finishedMappers;
	}
}