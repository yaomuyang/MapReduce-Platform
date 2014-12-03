package test;

import mapreduce.InputChunk;
import mapreduce.Job;
import mapreduce.Key;
import mapreduce.MapperInterface;
import mapreduce.Output;
import mapreduce.ReducerInterface;
import mapreduce.Values;

/* programmer defined class */
public class FailureTest {

	/* 
	 * the functions to be performed by map() must be programmer defined.
	 * The user defined mapper class must implement the mapper interface
	 */
	public static class Mapper implements MapperInterface{	
		
		/* the map() method. A copy of this method is invoked by each mapper */
		public void map(Key key,InputChunk input, Output output) throws Exception{
			
			double a = Math.random();
			if(a>0.995){
				throw new Exception();
			}
		}
	}
	
	public static class Reducer implements ReducerInterface{
		
		public void reduce(Key key, Values values, Output output) throws Exception{
			;
		}
	}


	public static void main(String[] args){		
		/* The entire map reduce process is exposed to the user only through the Job class.
		 * The user just needs set the necessary information for the application submission and invoke the runJob method */
		try{
			Job myJob = new Job("HelloJob");
			myJob.setMapClass(Mapper.class);
			myJob.setReduceClass(Reducer.class);
			myJob.setInputFile("oldmanandthesea.txt");
			myJob.setOutputFile("Output.txt");
			myJob.runJob(args);
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
}