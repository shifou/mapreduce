package hdfs;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class HDFSFile implements Serializable {

	private static final long serialVersionUID = 3326499942746127733L;
	public String filename;
	public ConcurrentHashMap<Integer, HDFSBlock> blocks;
	public ConcurrentHashMap<Integer, HDFSBlock> getBlockList() {
		return blocks;
	}
	public int getBlockSize(){
		return blocks.size();
	}
	public HDFSFile(String filename)
	{
		this.filename=filename;
	}
}
