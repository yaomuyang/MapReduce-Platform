package info;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ApplicationId implements Serializable {
	public int id = 0;
	
	public boolean equals(Object obj){
		ApplicationId applicationId = (ApplicationId)obj;
		if(this.id == applicationId.id){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode(){
		return this.id;
	}
}