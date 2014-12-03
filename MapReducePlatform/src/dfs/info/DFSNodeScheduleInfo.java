package dfs.info;

import java.io.Serializable;
import java.net.Socket;
import java.util.HashMap;

/*
 * The core data structure used to maintain the status of a node in DFSScheduler
 */
@SuppressWarnings("serial")
public class DFSNodeScheduleInfo extends DFSNode implements Serializable{
	private HashMap<DFSChunkId, DFSChunkInfo> chunkStatus;
	transient private Socket s;
	
	public DFSNodeScheduleInfo(DFSNode node){
		super(node);
		this.chunkStatus = new HashMap<DFSChunkId, DFSChunkInfo>();
	}	
	
	public void addChunk(DFSChunkInfo chunk){
		this.chunkStatus.put(chunk.getChunkId(), chunk);
	}
	
	public Socket getSocket(){
		return this.s;
	}
	
	public void setSocket(Socket s){
		this.s = s;
	}
}