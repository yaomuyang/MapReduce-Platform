package dfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import commons.Communication;
import commons.Configuration;
import dfs.info.DFSChunkId;
import dfs.info.DFSNode;
import dfs.info.DFSNodeId;

/*
 * DataNode is the client node of DFS, here we used the same name as HDFS
 * It is the place that chunks of DFS files are actually stored
 */
public class DataNode {
	
	private DFSNode node;
	private String folderName;
	private Configuration conf;
	
	private ServerSocket ss;
	
	private HashSet<DFSChunkId> chunks;

	public static void main(String[] args){
		try{
			DataNode dataNode = new DataNode();
			dataNode.run();
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
			
	DataNode() throws Exception{
		//set up the configuration
		this.conf = new Configuration();
		System.out.println("[DataNode]: Configuration set up");
		
		//Establish the server socket to listen for commands
		ss = new ServerSocket(Integer.parseInt(conf.getProperty("DataNodePort")));
		System.out.println("[DataNode]: Set up the server socket to listen for storing or requiring a chunk");
		
		this.chunks = new HashSet<DFSChunkId>();
	}
			
	/*
	 * This function will open a port and listen for commands, and create a new thread to deal with the upcomming socket connection
	 */
	public void run() throws Exception{	
		//register the node to namenode
		this.registerNode();
		while(true){
			Socket s = ss.accept();
			Thread tempThread = new Thread(new DataNodeSocketRunnable(s));
			tempThread.start();
		}
	}
	
	/*
	 * Register Node function, this is called when a datanode is first established
	 * we connect to the namenode and register this datanode, as a usable DFS client
	 */
	private void registerNode() throws Exception{
		
		System.out.println("[DataNode]: Registering this datanode to namenode");
		
		//send out register command and receive for registered node id
		Socket s = new Socket(conf.getProperty("NameNodeIP"), Integer.parseInt(conf.getProperty("NameNodePort")));
		Communication.sendString("register", s);
		this.node = new DFSNode(InetAddress.getLocalHost());
		Communication.sendObject(this.node, s);
		DFSNodeId nodeId = (DFSNodeId)Communication.receiveObject(s);
		
		//set node id and create DFS backend folder
		this.node.setId(nodeId.id);
		this.folderName = "DFSBackend" + nodeId.id;
		File file = new File(this.folderName);
		file.mkdir();
		
	}
	
	/*
	 * The thread that deal with the communication with a single client
	 */
	class DataNodeSocketRunnable implements Runnable{
		
		Socket s;
		
		DataNodeSocketRunnable(Socket s){
			this.s = s;
		}	
		
		/*
		 * In this thread, I will deal with a certain socket connection to the datanode
		 * We will loop once and once again to receive request from the same socket
		 * If we receive a command, we will call the function to deal with it
		 */
		public void run(){
			try{
				while(true){
					String command = Communication.receiveString(s);
					if(command.equals("replicate")){
						//If the command is "replicate"
						this.storeChunk(s);
					}else if(command.equals("acquire")){
						//If the command is "acquire"
						this.acquireChunk(s);
					}else if(command.equals("delete")){
						//If the command is "delete"
						this.deleteChunk(s);
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		
		/*
		 * Receive chunk from the namenode to store on this node
		 * We store the chunkinfo in the local data structure and store the chunk into the file system
		 */
		private void storeChunk(Socket s) throws Exception{
			//receive chunk information
			DFSChunkId chunkId = (DFSChunkId)Communication.receiveObject(s);
			chunks.add(chunkId);
			System.out.println("[DataNode]: A new chunk is sent here to store, with id: "+chunkId.id);
			//receive the chunk data and store to local filesystem
			byte[] bytes = Communication.receiveBytes(s);
			String fileName = folderName + "/Chunk" + chunkId.id;
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fileName));
			bos.write(bytes);
			bos.flush();
			bos.close();
		}
		
		/*
		 * Delete a specific chunk on this node
		 * Delete the chunk on the local file system and delete the local data structure related to that chunk
		 */
		private void deleteChunk(Socket s) throws Exception{
			//receive chunk information
			ArrayList<DFSChunkId> chunkIds = (ArrayList<DFSChunkId>)Communication.receiveObject(s);
			for(DFSChunkId chunkId: chunkIds){
				System.out.println("[DataNode]: Someone is deleting chunk "+chunkId.id);
				if(chunks.contains(chunkId)){
					//delete the local chunk
					String fileName = folderName + "/Chunk" + chunkId.id;
					File chunkFile = new File(fileName);
					chunkFile.delete();
					chunks.remove(chunkId);
				}
			}
		}
		
		/*
		 * A request for chunks to this datanode
		 * We read the chunk and then return the byte array to the requester
		 */
		private void acquireChunk(Socket s) throws Exception{
			//receive chunk information
			DFSChunkId chunkId = (DFSChunkId)Communication.receiveObject(s);
			System.out.println("[DataNode]: Some mapper is acquiring chunk "+chunkId.id);
			if(chunks.contains(chunkId)){
				//read the chunk and send it by socket
				String fileName = folderName + "/Chunk" + chunkId.id;
				File chunkFile = new File(fileName);
				byte[] bytes = new byte[(int)chunkFile.length()];
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(chunkFile));
				bis.read(bytes, 0, bytes.length);
				bis.close();
				Communication.sendBytes(s, bytes);
			}
		}
	}
}