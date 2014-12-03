package dfs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import commons.Communication;
import commons.Configuration;
import dfs.info.DFSChunkId;
import dfs.info.DFSChunkInfo;
import dfs.info.DFSFileId;
import dfs.info.DFSNode;
import dfs.info.DFSNodeId;
import dfs.info.DFSNodeScheduleInfo;
import dfs.info.DFSFileInfo;
import dfs.info.DFSStatus;

public class DFSScheduler {
	
	private static int dataNodePort;
	
	private static int incrementalDFSNodeId;
	private static int incrementalFileId;	
	private static int incrementalChunkId;
	
	private static int replicaNumbers;
	private static int defaultReplicaNumbers;
	private static int defaultRecordsPerChunk;
	
	private static ArrayList<DFSNodeScheduleInfo> nodes;
	private static HashMap<String, DFSFileId> fileNames;
	private static HashMap<DFSFileId, DFSFileInfo> files;
	
	/*
	 * Initiate the parameters of the scheduler according to the configuration file
	 */
	public void initiate(Configuration conf){
		DFSScheduler.dataNodePort = Integer.parseInt(conf.getProperty("DataNodePort"));
		DFSScheduler.incrementalFileId = 1;
		DFSScheduler.incrementalChunkId = 1;
		DFSScheduler.incrementalDFSNodeId = 1;
		DFSScheduler.replicaNumbers = 0;
		DFSScheduler.defaultReplicaNumbers = Integer.parseInt(conf.getProperty("ReplicaNumber"));
		DFSScheduler.defaultRecordsPerChunk = Integer.parseInt(conf.getProperty("RecordPerChunk"));
		DFSScheduler.nodes = new ArrayList<DFSNodeScheduleInfo>();
		DFSScheduler.fileNames = new HashMap<String, DFSFileId>();
		DFSScheduler.files = new HashMap<DFSFileId, DFSFileInfo>();
	}
	
	public int getReplicaNumbers(){
		return DFSScheduler.replicaNumbers;
	}
	
	/*
	 * Interface to add a file to the DFS
	 * We will schedule this file to be broken into chunks and sent to the datanodes to store
	 */
	public void addFile(String fileName, byte[] bytes) throws Exception{
		DFSFileInfo file = new DFSFileInfo(DFSScheduler.incrementalFileId, fileName);
		DFSScheduler.incrementalFileId = DFSScheduler.incrementalFileId + 1;
		fileNames.put(fileName, file.getFileId());
		files.put(file.getFileId(), file);
		System.out.println("[NameNode - DFSScheduler]: New file : "+fileName+" is assigned a file Id: "+file.getFileId().id);
		this.breakFile(file, bytes);
	}

	/*
	 * After we broke the byte[] into chunks, we create the chunk info, and assign chunk id
	 * Also, we add this chunk to our local datastructure
	 * For each of the chunks, we call replicateChunk() function to replicate this chunk to multiple datanodes
	 */
	private void breakFile(DFSFileInfo fileInfo, byte[] bytes) throws Exception{
		ArrayList<byte[]> chunkBytes = this.breakChunks(bytes);
		System.out.println("[NameNode - DFSScheduler]: New file with id :"+fileInfo.getFileId().id+" is broken into "+ chunkBytes.size()+" chunks");
		ArrayList<DFSChunkInfo> chunks = new ArrayList<DFSChunkInfo>();
		for(byte[] chunByte: chunkBytes){
			DFSChunkInfo chunk = new DFSChunkInfo(DFSScheduler.incrementalChunkId);
			DFSScheduler.incrementalChunkId = DFSScheduler.incrementalChunkId + 1;
			chunks.add(chunk);	
			this.replicateChunk(chunk, chunByte);			
		}
		fileInfo.setChunks(chunks);	
		
	}
	
	/*
	 * This function will make replications of the chunk, and assign datanodes to store this node
	 */
	private void replicateChunk(DFSChunkInfo chunk, byte[] bytes){
		HashSet<Integer> usedNodes = new HashSet<Integer>();
		//relicate the chunk to several datanodes
		for(int replicated=0; replicated<DFSScheduler.replicaNumbers; ){
			int tempNode = (int)(Math.random()*DFSScheduler.nodes.size());
			if(usedNodes.contains(tempNode)){
				continue;
			}
			try{
				replicated++;
				usedNodes.add(tempNode);
				DFSNodeScheduleInfo node = DFSScheduler.nodes.get(tempNode); 
				//Add the chunkInfo to the nodeInfo, as well as adding the nodeInfo into the chunkInfo
				chunk.addNode(node);
				node.addChunk(chunk);
				//send the chunk to the datanode
				sendChunk(node, chunk, bytes);
			}catch(Exception e){
				//deal with this problem here later
				System.out.println("*****************connection error: sending chunk to datanode");
				e.printStackTrace();
			}	
		}
	}
	
	/*
	 * This function eventually send the chunk data to the data node for storage
	 */
	private void sendChunk(DFSNodeScheduleInfo node, DFSChunkInfo chunk, byte[] bytes) throws Exception{
		Socket s;
		if(node.getSocket()==null){
			s = new Socket(node.getInetAddress(), DFSScheduler.dataNodePort);
			node.setSocket(s);
		}else{
			s=node.getSocket();
		}
		Communication.sendString("replicate", s);
		Communication.sendObject(chunk.getChunkId(), s);
		Communication.sendBytes(s, bytes);
	}
	
	/*
	 * This function is an interface that we delete a file on DFS
	 * We delete the file on our data structure
	 * Also, we delete everything on the datanodes related to this file
	 */
	public void deleteFile(String fileName){
		DFSFileId fileId = fileNames.get(fileName);
		DFSFileInfo fileInfo = files.get(fileId);
		ArrayList<DFSChunkInfo> chunks = fileInfo.getChunks();
		//HashMap the chunks to different datanodes and ask them to delete them
		HashMap<DFSNode, ArrayList<DFSChunkId>> chunkIds = new HashMap<DFSNode, ArrayList<DFSChunkId>>();
		for(DFSChunkInfo chunk: chunks){
			ArrayList<DFSNode> nodesOfChunk = chunk.getNodes();
			for(DFSNode nodeOfChunk: nodesOfChunk){
				if(!chunkIds.containsKey(nodeOfChunk)){
					chunkIds.put(nodeOfChunk, new ArrayList<DFSChunkId>());							
				}
				chunkIds.get(nodeOfChunk).add(chunk.getChunkId());
			}
		}
		//for each of the nodes, send them the list of chunks to delete
		for(DFSNode nodeToDelete: chunkIds.keySet()){
			try{
				sendDelete(nodeToDelete, chunkIds.get(nodeToDelete));
			}catch(Exception e){
				//deal with this problem here later
				System.out.println("*****************connection error: deleting chunks datanode");
				e.printStackTrace();
			}
		}	
		//clean the local data structure
		fileNames.remove(fileName);
		files.remove(fileId);
	}
	
	/*
	 * This function eventually send the delete command to the datanode
	 */
	private void sendDelete(DFSNode node, ArrayList<DFSChunkId> chunkIds) throws Exception{
		Socket s = new Socket(node.getInetAddress(), DFSScheduler.dataNodePort);
		Communication.sendString("delete", s);
		Communication.sendObject(chunkIds, s);
	}
	
	/*
	 * This function is an interface that we download a file from the DFS
	 */
	public byte[] getFile(String fileName) throws Exception{
		System.out.println("[NameNode - DFSScheduler]: Getting file : "+fileName);
		DFSFileId fileId = fileNames.get(fileName);
		DFSFileInfo fileInfo = files.get(fileId);
		ArrayList<DFSChunkInfo> chunks = fileInfo.getChunks();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		//for each of the chunks, we just call the getChunk() function to get the byte[] of the chunk
		for(DFSChunkInfo chunk: chunks){
			byte[] bytes = this.getChunk(chunk);
			out.write(bytes);
		}
		return out.toByteArray();
	}
	
	/*
	 * getChunk() function: the same of NodeManager
	 * Get the byte[] of a chunk using the DFSChunkInfo
	 */
	private byte[] getChunk(DFSChunkInfo chunk) throws Exception{
		for(DFSNode node : chunk.getNodes()){
			try{
				Socket s = new Socket(node.getInetAddress(), dataNodePort);
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
	
	/*
	 * Deal with the registration of a data node
	 * Store this into the local datastructure as a usable datanode
	 */
	public DFSNodeId registerNode(DFSNode node){
		DFSNodeScheduleInfo nodeInfo = new DFSNodeScheduleInfo(node);
		nodeInfo.setId(incrementalDFSNodeId);
		incrementalDFSNodeId = incrementalDFSNodeId + 1;
		System.out.println("[NameNode - DFSScheduler]: Node with ip: "+node.getInetAddress().getHostAddress()+" registerd, assigned node id: "+nodeInfo.getId().id);
		nodes.add(nodeInfo);
		DFSScheduler.replicaNumbers = Math.min(nodes.size(), DFSScheduler.defaultReplicaNumbers);
		return nodeInfo.getId();
	}
	
	/*
	 * This function constructs the inforamtion that will be sent back to the DFSClient, to be displayed to end-users
	 */
	public DFSStatus getDFSStatus(){
		return new DFSStatus(replicaNumbers, nodes.size(), new ArrayList<String>(fileNames.keySet()));
	}
	
	/*
	 * This function returns the fileInfo of a file
	 * When creating a mapreduce job, we would need the file info of the input file. 
	 */
	public DFSFileInfo getFileInfo(String fileName){
		DFSFileId fileId = fileNames.get(fileName);
		return files.get(fileId);
	}
	
	/*
	 * A tool function to break a file into chunks, according to the recordSize
	 */
	private ArrayList<byte[]> breakChunks(byte[] fileBytes) throws Exception{
		ArrayList<byte[]> chunkBytes = new ArrayList<byte[]>();

		String text = new String(fileBytes);
		BufferedReader buffReader = new BufferedReader(new StringReader(text));
		String currentLine;
		
		int recordCount = 0;
		String stringChunk = "";
		while((currentLine = buffReader.readLine())!=null){
			
			//store the chunk data if we got the record size
			if(recordCount==defaultRecordsPerChunk){
				byte[] chunk = new byte[stringChunk.length()];
				chunk = stringChunk.getBytes();
				chunkBytes.add(chunk);
				recordCount=0;
				stringChunk = "";
			}

			stringChunk = stringChunk + currentLine + '\n';
			recordCount++;	
		}
		
		byte[] chunk = new byte[stringChunk.length()];
		chunk = stringChunk.getBytes();
		chunkBytes.add(chunk);
		
		return chunkBytes;
	}
}