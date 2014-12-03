package info;

import java.io.Serializable;

@SuppressWarnings("serial")
public class NodeId implements Serializable {
	public int id = 0;
	
	public boolean equals(Object obj){
		NodeId nodeId = (NodeId)obj;
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