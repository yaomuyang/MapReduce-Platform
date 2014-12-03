package dfs;

import java.net.ServerSocket;
import java.net.Socket;
import commons.Communication;
import commons.Configuration;
import dfs.info.DFSFileInfo;
import dfs.info.DFSNode;
import dfs.info.DFSNodeId;

/*
 * Controller of the DFS, receive DFS commands and deal with them accordingly
 */
public class NameNode {
	
	//configuration generated from the config file
	Configuration conf;
	
	//scheduler of the DFS
	DFSScheduler scheduler;

	public static void main(String[] args){
		try{
			NameNode nameNode = new NameNode();
			nameNode.run();
		}catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
			
	NameNode() throws Exception{
		this.conf = new Configuration();
		System.out.println("[NameNode]: Configuration set up");
		
		this.scheduler = new DFSScheduler();
		this.scheduler.initiate(this.conf);
		System.out.println("[NameNode]: DFS Scheduler initiated");
	}
			
	public void run() throws Exception{				
		HandleCommandRunnable handleCommandRunnable = new HandleCommandRunnable();
		Thread handleCommandThread = new Thread(handleCommandRunnable);
		handleCommandThread.start();		
	}
	
	/*
	 * This thread handles the incomming commands, and call different functions to deal with them accordingly
	 */
	class HandleCommandRunnable implements Runnable{
		
		ServerSocket ss;
		
		HandleCommandRunnable() throws Exception{
			ss = new ServerSocket(Integer.parseInt(conf.getProperty("NameNodePort")));
			System.out.println("[NameNode - HandleCommand]: Set up the server socket to listen for commands from the client");
		}
		
		public void run(){
			try{				
				while(true){
					//Accept the new command
					Socket s = ss.accept();
					String command = Communication.receiveString(s);
					System.out.println("[NameNode - HandleCommand]: New "+command+" command received");
					if(command.equals("upload")){
						this.uploadFile(s);
					}else if(command.equals("delete")){
						this.deleteFile(s);
					}else if(command.equals("get")){
						this.getFile(s);
					}else if(command.equals("list")){
						Communication.sendObject(scheduler.getDFSStatus(), s);
					}else if(command.equals("register")){
						this.registerNode(s);
					}else if(command.equals("getfile")){
						this.getFileInfo(s);
					}else{
						//this is abnormal
					}					
				}
			}catch(Exception e){
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		/*
		 * This function is called when the user wants to upload a file to DFS
		 */
		private void uploadFile(Socket s) throws Exception{
			try{
				String fileName = Communication.receiveString(s);
				byte[] bytes = Communication.receiveBytes(s);
				System.out.println("[NameNode - HandleCommand]: New file : "+fileName+" is uploaded to the DFS");
				scheduler.addFile(fileName, bytes);
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				Communication.sendObject(scheduler.getDFSStatus(), s);
			}
		}
		
		/*
		 * This function is called when the user wants to delete a file on the DFS
		 */
		private void deleteFile(Socket s) throws Exception{
			try{
				String fileName = Communication.receiveString(s);
				scheduler.deleteFile(fileName);
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				Communication.sendObject(scheduler.getDFSStatus(), s);
			}
		}
		
		/*
		 * This function is called when the user wants to download a file on the DFS
		 */
		private void getFile(Socket s) throws Exception{
			try{
				String fileName = Communication.receiveString(s);
				byte[] bytes = scheduler.getFile(fileName);
				Communication.sendBytes(s, bytes);;	
			}catch(Exception e){
				e.printStackTrace();
				Communication.sendBytes(s, new byte[0]);
			}finally{
				Communication.sendObject(scheduler.getDFSStatus(), s);
			}
		}
		
		/*
		 * This function is called when a new datanode registers
		 */
		private void registerNode(Socket s) throws Exception{
			DFSNode node = (DFSNode)Communication.receiveObject(s);
			node.setAddress(s.getInetAddress());
			System.out.println("[NameNode - HandleCommand]: Node with ip: "+node.getInetAddress().getHostAddress()+" is registring as a new datanode");
			DFSNodeId nodeId = scheduler.registerNode(node);
			Communication.sendObject(nodeId, s);
		}
		
		/*
		 * This function is called when the resoucemanager asks for the status for the inputfile of a mapreduce job
		 */
		private void getFileInfo(Socket s) throws Exception{
			String fileName = Communication.receiveString(s);
			DFSFileInfo file = scheduler.getFileInfo(fileName);
			System.out.println("Debug: "+file.getChunks());
			//Communication.sendObject(file.getChunks().get(0).getNodes().get(0), s);
			Communication.sendObject(file, s);
		}
	}
}