package dfs.info;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DFSFileId implements Serializable {
	public int id = 0;
	
	public boolean equals(Object obj){
		DFSFileId nodeId = (DFSFileId)obj;
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