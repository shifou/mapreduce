package mapreduce;

import hdfs.DataNodeInfo;
import hdfs.HDFSBlock;

import java.io.Serializable;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class InputSplit implements Serializable {

	private static final long serialVersionUID = -3216548064249420892L;
	private HDFSBlock block;
	private int fileid;

	public InputSplit(int id, HDFSBlock hold) {
		fileid = id;
		setBlock(hold);
	}

	public HDFSBlock getBlock() {
		return block;
	}

	public void setBlock(HDFSBlock block) {
		this.block = block;
	}
	
	public HashSet<String> getLocations(){
		return this.block.getLocations();
	}
}
