package dfs.info;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DFSChunkId implements Serializable{
	public int id = 0;
	public DFSFileId fileId = new DFSFileId();
	
	public boolean equals(Object obj){
		DFSChunkId nodeId = (DFSChunkId)obj;
		if(this.id == nodeId.id){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode(){
		return this.id;
	}
}