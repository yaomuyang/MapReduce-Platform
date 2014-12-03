package info;

import java.io.Serializable;
import dfs.info.DFSChunkInfo;
import mapreduce.Output;

/* 
 * This structure represents the information necessary to instantiate a mapper 
 */
@SuppressWarnings("serial")
public class MapperInfo extends JobConf implements Serializable {

	private Class<?> mapperClass;
	private DFSChunkInfo chunk;
	
	Output output;
	
	/* we can have a replica list also - the set of worker nodes where the input file replicas are located */
	
	public MapperInfo(DFSChunkInfo chunk, Class<?> mapperClass){
		super();
		this.chunk = chunk;
		this.mapperClass = mapperClass;
	}
	
	public MapperInfo(){
		super();
	}
	
	public void setMapperClass(Class<?> mapperClass){
		this.mapperClass = mapperClass;
	}
	
	public Class<?> getMapperClass(){
		return this.mapperClass;
	}
	
	public void setOutput(Output output){
		this.output = output;
	}
	
	public Output getOutput(){
		return this.output;
	}
	
	public void setChunk(DFSChunkInfo chunk){
		this.chunk = chunk;
	}
	
	public DFSChunkInfo getChunk(){
		return this.chunk;
	}
}