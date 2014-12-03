package dfs.info;

import java.io.Serializable;
import java.util.ArrayList;

/*
 * This DFSChunkInfo is a major datastructure that will be used both in DFS and in MapReduce
 * When initiating a new mapper in NodeManager, this infomation should be used to access the chunk of that mapreduce job
 */
@SuppressWarnings("serial")
public class DFSChunkInfo implements Serializable{
	private DFSChunkId id;
	//a list of nodes that this chunk may resides in
	private ArrayList<DFSNode> nodes;
	
	public DFSChunkInfo(int chunkId){
		this.id = new DFSChunkId();
		this.id.id = chunkId;
		this.nodes = new ArrayList<DFSNode>();
	}
	
	public void addNode(DFSNode node){
		this.nodes.add(node);
	}
	
	public DFSChunkId getChunkId(){
		return this.id;
	}
	
	public ArrayList<DFSNode> getNodes(){
		return this.nodes;
	}
}