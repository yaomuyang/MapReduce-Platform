package test;

import java.util.StringTokenizer;

import mapreduce.InputChunk;
import mapreduce.Job;
import mapreduce.Key;
import mapreduce.MapperInterface;
import mapreduce.Output;
import mapreduce.ReducerInterface;
import mapreduce.Values;


/* programmer defined class */
public class ApplicationTest {

	/* 
	 * the functions to be performed by map() must be programmer defined.
	 * The user defined mapper class must implement the mapper interface
	 */
	public static class HelloMapper implements MapperInterface{	
		
		/* the map() method. A copy of this method is invoked by each mapper */
		public void map(Key key,InputChunk input, Output output) throws Exception{
			String inputString = input.toString();
			//System.out.println("[JobClient:] input: "+inputString);
			StringTokenizer tokenizer = new StringTokenizer(inputString);
			while(tokenizer.hasMoreTokens())
			{
				key.setKey(tokenizer.nextToken());
				//collect the key value pairs
				output.write(key,1);
			}

			/*
			double a = Math.random();
			//System.out.println(a);
			if(a>0.99){
				throw new Exception();
			}
			*/
		}
	}
	
	public static class HelloReducer implements ReducerInterface{
		
		public void reduce(Key key, Values values, Output output) throws Exception{
			int sum = 0;
			while(values.hasNext()){
				sum = sum + values.getNextValue();
			}
			output.write(key,sum);
		}
	}


	public static void main(String[] args){		
		/* The entire map reduce process is exposed to the user only through the Job class.
		 * The user just needs set the necessary information for the application submission and invoke the runJob method */
		try{
			Job myJob = new Job("HelloJob");
			myJob.setMapClass(HelloMapper.class);
			myJob.setReduceClass(HelloReducer.class);
			myJob.setInputFile("oldmanandthesea.txt");
			myJob.setOutputFile("Output.txt");
			myJob.runJob(args);
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
}