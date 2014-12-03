package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.net.Socket;
import commons.Communication;
import commons.Configuration;
import dfs.info.DFSFileInfo;
import dfs.info.DFSStatus;

public class DFSClient {
	
	private Socket s;
	
	/*
	 * Main function of the DFSClient, may be used when user want to access DFS using command line
	 */
	public static void main(String[] args) throws Exception{
		
		if(args.length<3){
			exitOnArgumentsError();
		}
		
		DFSClient dfsClient = new DFSClient(args[0], args[1]);
		
		DFSStatus status = null;
		//send the command
		String commandName = args[2];
		if(commandName.equals("upload")){
			status = dfsClient.uploadFile(args[3]);
		}else if(commandName.equals("download")){
			status = dfsClient.getFile(args[3]);
		}else if(commandName.equals("delete")){
			status = dfsClient.deleteFile(args[3]);
		}else if(commandName.equals("list")){
			status = dfsClient.listFile();
		}else{
			System.out.println("debug"+commandName);
			exitOnArgumentsError();
		}
		System.out.println("[DFSClient]: DFS Status Report:\n" + status.toString());
	}
	
	/*
	 * Constructor for other components to user, like NodeManager, in specific
	 */
	public DFSClient(Configuration conf) throws Exception{
		this.s = new Socket(conf.getProperty("NameNodeIP"), Integer.parseInt(conf.getProperty("NameNodePort")));
		System.out.println("[DFSClient]: connecting to namenode");
	}
	
	/*
	 * Constructor for the command line input
	 */
	public DFSClient(String nameNodeIP, String nameNodePort) throws Exception{
		this.s = new Socket(nameNodeIP, Integer.parseInt(nameNodePort));
		System.out.println("[DFSClient]: connecting to namenode");
	}
	
	public static void exitOnArgumentsError(){
		System.out.println("Argument Error: Usage java DFSClient namenode_ip namenode_port command command_specific_parameters");
		System.out.println("legal commands: upload filePath / delete fileName / download fileName / list");
		System.exit(1);
	}
	
	/*
	 * upload a specific file to the DFS
	 * It will first send the file to the namenode, break it into chunks, and them replicate among datanodes
	 */
	public DFSStatus uploadFile (String filePath) throws Exception{
		//read the file into memory
		File uploadFile = new File(filePath);
		byte[] bytes = new byte[(int)uploadFile.length()];
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(uploadFile));
		bis.read(bytes, 0, bytes.length);
		bis.close();
		//send the file over to the namenode
		System.out.println("[DFSClient]: Sending file to namenode, filename = "+uploadFile.getName());
		Communication.sendString("upload", this.s);
		Communication.sendString(uploadFile.getName(), this.s);
		Communication.sendBytes(this.s, bytes);
		return (DFSStatus)Communication.receiveObject(this.s);
	}
	
	/*
	 * delete a file on the DFS
	 * This will delete the information about the file, as well as all the chunks on the datanode
	 */
	public DFSStatus deleteFile(String fileName) throws Exception{
		//send the command and the filename to the namenode
		Communication.sendString("delete", s);
		Communication.sendString(fileName, s);
		return (DFSStatus)Communication.receiveObject(s);
	}
	
	/*
	 * get a file from the DFS to the local file system
	 */
	public DFSStatus getFile(String fileName) throws Exception{
		Communication.sendString("get", s);
		Communication.sendString(fileName, s);
		byte[] bytes = Communication.receiveBytes(s);
		writeFile(fileName, bytes);
		return (DFSStatus)Communication.receiveObject(s);
	}
	
	private void writeFile(String fileName, byte[] bytes) throws Exception{
		File file = new File("DFSDownload");
		file.mkdir();
		FileOutputStream fos = new FileOutputStream("DFSDownload/"+fileName);
		fos.write(bytes);
		fos.close();
	}
	
	/*
	 * list the files on the DFS
	 */
	public DFSStatus listFile() throws Exception{
		Communication.sendString("list", s);
		return (DFSStatus)Communication.receiveObject(s);
	}
	
	/*
	 * Get all the information about a file on DFS
	 */
	public DFSFileInfo getFileInfo(String fileName) throws Exception{
		Communication.sendString("getfile", s);
		Communication.sendString(fileName, s);
		return (DFSFileInfo)Communication.receiveObject(s);
	}
}