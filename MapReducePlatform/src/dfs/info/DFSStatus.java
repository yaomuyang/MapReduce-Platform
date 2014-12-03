package dfs.info;

import java.io.Serializable;
import java.util.ArrayList;

/*
 * This class contains the main informations of the DFS, to be shown to the user
 */
@SuppressWarnings("serial")
public class DFSStatus implements Serializable{
	private int replications;
	private int nodes;
	private ArrayList<String> fileNames;
	
	public DFSStatus(int replications, int nodes, ArrayList<String> fileNames){
		this.replications = replications;
		this.nodes = nodes;
		this.fileNames = fileNames;
	}
	
	public String toString(){
		String str = "";
		str = str + "[DFSClient: Status Report]: Alive DataNodes: " + this.nodes + '\n';
		str = str + "[DFSClient: Status Report]: Replications: " + this.replications + '\n';
		str = str + "[DFSClient: Status Report]: Files in DFS:\n";
		int i = 1;
		for(String fileName: fileNames){
			str = str + "[DFSClient: Status Report]: "+i+"\t"+fileName + '\n';
			i = i + 1;
		}
		return str;
	}
}