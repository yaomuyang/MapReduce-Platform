package info;

import java.io.Serializable;
import java.net.InetAddress;

@SuppressWarnings("serial")
public class ReducerInput implements Serializable {

	private String inputFileName;
	private InetAddress inputAddress;
	
	public ReducerInput(String inputFileName, InetAddress inputAddress){
		this.inputAddress = inputAddress;
		this.inputFileName = inputFileName;
	}
	
	public InetAddress getAddress(){
		return this.inputAddress;
	}
	
	public String getFileName(){
		return this.inputFileName;
	}
}