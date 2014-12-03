package nodemanager;

import java.io.BufferedReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.ArrayList;

import info.ReducerInfo;
import info.ReducerInput;
import commons.Communication;
import commons.Configuration;
import mapreduce.Key;
import mapreduce.Values;
import mapreduce.Output;

public class ReducerRunnable implements Runnable{
	
	ReducerInfo reducerInfo;
	ThreadInfo.ThreadCallBack callback;
	Configuration conf;
	
	ReducerRunnable(ReducerInfo reducerInfo, ThreadInfo.ThreadCallBack callback, Configuration conf){
		this.reducerInfo = reducerInfo;
		this.callback = callback;
		this.conf = conf;
	}
	
	public void run(){
		try{
			
			/* instantiate the user defined reducer class */
			System.out.println("Debug: "+this.reducerInfo.getReducerClass());
			Object reduceObject = this.reducerInfo.getReducerClass().newInstance();
			
			/* get reduce method */
			Method reduceMethod = reducerInfo.getReducerClass().getDeclaredMethod("reduce", Key.class, Values.class, Output.class);
			
			/* open file handles of intermediate files */
			reducerInfo.getOutput().openFileHandles();
			
			KeyValueList keyValueListObject = structureKeyValues();
			
			/* for each of those [key, raw value] pairs invoke the reduce method
			*/
			
			ArrayList<KeyValueCounts> kvcList = keyValueListObject.keyValueList;
			for(KeyValueCounts kvc : kvcList ){
				Key key = kvc.key;
				Values values = kvc.values;
				reduceMethod.invoke(reduceObject, key, values, reducerInfo.getOutput());
			}

			reducerInfo.getOutput().closeFileHandles();
			
		}catch(Exception e){
			e.printStackTrace();
			callback.failJob();
		}
	}

	private KeyValueList structureKeyValues() throws Exception{
		
		/* get the input files of the reducer */
		ArrayList<ReducerInput> reducerInputList = this.reducerInfo.getReducerInputList();
		
		/* An object that contains the keys and the raw value list 
		 * KeyValueCounts 1 : key1 --> [1,1,1,1]
		 * KeyValueCounts2: key2 --> [1,1,1]
		 * keyValueListObject : keyValueCounts1,keyValueCounts2...keyValueCountsn
		*/
		KeyValueList keyValueListObject = new KeyValueList();
		
		//get reducer's input i.e. intermediate files
		for(int fileCount=0; fileCount<reducerInputList.size(); fileCount++){
			// connect to the data location to get the intermediate file */
			ReducerInput reducerInput = reducerInputList.get(fileCount);
			//Socket s = new Socket(reducerInput.inputFileLocationIP,4545);
			//Communication.sendString(reducerInput.inputFileName,s);
			//byte[] inputFileBytes = Communication.receiveBytes(s);
			
			byte[] inputFileBytes = this.getIntermediateFile(reducerInput);
			//System.out.println("[NodeManager - ReducerRunnable]: Got intermediate file: "+reducerInput.getFileName()+" from "+reducerInput.getAddress().getHostAddress());
		
			/* convert the intermediate file contents to string to parse it */
			String text = new String(inputFileBytes);
					
			BufferedReader buffReader = new BufferedReader(new StringReader(text));
			String currentLine;

			/* find the keys and make a raw list of values for each key */		
			/* read each line in the intermediate file */
			while((currentLine = buffReader.readLine())!=null){
				
				/* break the line to extract key and value */
				String[] key_value = currentLine.split(" ");
				String inputKey = key_value[0];
				int valueCount = Integer.parseInt(key_value[1]);
					
				/* if the key already exists in the KeyValueListObject, just accumulate to its raw count */
				if(keyValueListObject.find(inputKey)!=null){
					keyValueListObject.accumulate(inputKey,valueCount);
				}else{
					/* insert the key and value to the list */
					keyValueListObject.insert(inputKey,valueCount);
				}
			}
		}
		return keyValueListObject;
	}
	
	private byte[] getIntermediateFile(ReducerInput reducerInput) throws Exception{
		Socket s = new Socket(reducerInput.getAddress(), Integer.parseInt(conf.getProperty("NodeManagerPort")));
		Communication.sendString(reducerInput.getFileName(), s);
		byte[] bytes = Communication.receiveBytes(s);
		Communication.deleteSocket(s);
		return bytes;
	}
	
	/* This class maintains the list of keys and their raw counts ( values ) */
	class KeyValueList{
		ArrayList<KeyValueCounts> keyValueList;
		
		public KeyValueList(){
			this.keyValueList = new ArrayList<KeyValueCounts>();
		}
		
		/* function to check if the key already exists in the list */
		public KeyValueCounts find(String inputKey){
			if(keyValueList.size()!=0)
			for(KeyValueCounts k:keyValueList){
				if(k.key.getKey().equals(inputKey)){
					return k;
				}
			}
			return null;
		}
		
		public void accumulate(String inputKey,int valueCount){
			KeyValueCounts locator = find(inputKey);
			locator.values.getValueList().add(valueCount);
		}
		
		public void insert(String inputKey,int valueCount){
			Key key = new Key();
			key.setKey(inputKey);
			
			ArrayList<Integer> valueList = new ArrayList<Integer>();
			valueList.add(valueCount);
			
			Values values = new Values();
			values.setValueList(valueList);
			
			KeyValueCounts kvc = new KeyValueCounts();
			kvc.setKeyValueCounts(key, values);
			
			keyValueList.add(kvc);
		}
	}
	
	class KeyValueCounts {

		Key key;
		Values values;
		
		void setKeyValueCounts(Key key,Values values){
			this.key = key;
			this.values = values;
		}
	}
}