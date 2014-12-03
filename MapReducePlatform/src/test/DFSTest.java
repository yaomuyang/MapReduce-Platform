package test;

import commons.Configuration;
import dfs.DFSClient;

public class DFSTest {
	public static void main(String[] args){
		try{
			Configuration conf = new Configuration();
			DFSClient client = new DFSClient(conf);
			client.uploadFile("mapreduce.conf");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
