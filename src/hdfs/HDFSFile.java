package hdfs;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class HDFSFile implements Serializable {
	public String filename;
	public int replica;
	public int blocksize;
	public ConcurrentHashMap<Integer, HDFSBlock> blocks;
	public ConcurrentHashMap<Integer, HDFSBlock> getBlockList() {
		
		return blocks;
	}
	//public String 
}
