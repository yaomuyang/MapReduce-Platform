package info;

import java.io.Serializable;
import java.util.ArrayList;

import mapreduce.Output;

/* 
 * This structure represents the information necessary to instantiate a reducer
 */
@SuppressWarnings("serial")
public class ReducerInfo extends JobConf implements Serializable {

	private ArrayList<ReducerInput> reducerInputList;
	
	private Class<?> reducerClass;
	
	private Output output;
		
	public ReducerInfo(Output outputFile, Class<?> reducerClass){
		this.output = outputFile;
		this.reducerClass = reducerClass;
	}
		
	public void setReducerInputList(ArrayList<ReducerInput> list){
		this.reducerInputList = list;
	}
		
	public ArrayList<ReducerInput> getReducerInputList(){
		return this.reducerInputList;
	}
		
	public Class<?> getReducerClass(){
		return this.reducerClass;
	}	
	
	public Output getOutput(){
		return this.output;
	}
}