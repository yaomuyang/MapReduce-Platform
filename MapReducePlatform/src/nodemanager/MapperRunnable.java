package nodemanager;

import java.io.BufferedReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.Socket;

import commons.Configuration;
import commons.Communication;
import dfs.info.DFSChunkInfo;
import dfs.info.DFSNode;
import info.MapperInfo;
import mapreduce.InputChunk;
import mapreduce.Output;
import mapreduce.Key;

/* This class is to start a mapper process on the worker node */
public class MapperRunnable implements Runnable {
	
	/* 
	 * mapperInfo contains all the information required by the mapper
	 * 1. MapperClass - the application programmer defined Map functionality
	 * 2. mapperId
	 */
	MapperInfo mapperInfo;
	ThreadInfo.ThreadCallBack callback;
	Configuration conf;
	
	MapperRunnable(MapperInfo mapperInfo, ThreadInfo.ThreadCallBack callback, Configuration conf){
		this.mapperInfo = mapperInfo;
		this.callback = callback;
		this.conf = conf;
	}
	
	public void run(){
		try{			
			/* instantiate the user defined mapper class */
			Object mapObject = mapperInfo.getMapperClass().newInstance();
			
			/* invoke the map method */
			/* currently map method is a no argument method - will refine it to work with input chunks and output files */
			byte[] byteChunk = getChunk(mapperInfo.getChunk());
			//InputChunk inputChunk = new InputChunk(byteChunk);
			//System.out.println("[NodeManager - MapperRunnable]: Got chunk: "+mapperInfo.getChunk().getChunkId().id);
			
			Key key = new Key();
			
			Method mapMethod = mapperInfo.getMapperClass().getDeclaredMethod("map", Key.class, InputChunk.class, Output.class);
			
			/* open file handles of intermediate files */
			mapperInfo.getOutput().openFileHandles();
			
			String entireChunkAsString = new String(byteChunk);
			BufferedReader bufferedReader = new BufferedReader(new StringReader(entireChunkAsString));
			
			String line;
			while((line = bufferedReader.readLine())!=null)
			{
				InputChunk lineChunk = new InputChunk(line.getBytes());
				mapMethod.invoke(mapObject, key, lineChunk, mapperInfo.getOutput());
			}
			
			mapperInfo.getOutput().closeFileHandles();
			
		}catch(Exception e){
			e.printStackTrace();
			callback.failJob();
		}
	}
	
	private byte[] getChunk(DFSChunkInfo chunk) throws Exception{
		for(DFSNode node : chunk.getNodes()){
			try{
				Socket s = new Socket(node.getInetAddress(), Integer.parseInt(conf.getProperty("DataNodePort")));
				Communication.sendString("acquire", s);
				Communication.sendObject(chunk.getChunkId(), s);
				Communication.deleteSocket(s);
				return Communication.receiveBytes(s);
			}catch(Exception e){
				System.out.println("[NodeManager - MapperRunnable]: attempt to get chunk failed, try next node");
			}
		}
		System.out.println("[NodeManager - MapperRunnable]: Failed to get the file");
		throw new Exception("Failed to get the chunk file, chunk id: "+chunk.getChunkId().id);
	}
}