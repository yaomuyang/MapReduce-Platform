package dfs.info;

import java.io.Serializable;
import java.util.ArrayList;

/*
 * The DFSFileInfo stores all the informations of a file on DFS
 * Especially all the info of all the chunks that it breaks into
 */
@SuppressWarnings("serial")
public class DFSFileInfo implements Serializable {
	private DFSFileId id;
	//a list of nodes that this chunk may resides in
	private String fileName;
	private ArrayList<DFSChunkInfo> chunks = new ArrayList<DFSChunkInfo>();
	
	public DFSFileInfo(int fileId, String fileName){
		this.id = new DFSFileId();
		this.id.id = fileId;
		this.fileName = fileName;
	}
	
	public DFSFileId getFileId(){
		return this.id;
	}
	
	public void setChunks(ArrayList<DFSChunkInfo> chunks){
		this.chunks = chunks;
	}
	
	public ArrayList<DFSChunkInfo> getChunks(){
		return this.chunks;
	}
}