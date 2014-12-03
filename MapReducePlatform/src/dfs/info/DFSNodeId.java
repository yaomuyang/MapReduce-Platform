package dfs.info;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DFSNodeId implements Serializable {
	public int id = 0;
	
	public boolean equals(Object obj){
		DFSNodeId nodeId = (DFSNodeId)obj;
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