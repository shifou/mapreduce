package hdfs;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HDFSFile implements Serializable {

	private static final long serialVersionUID = 3326499942746127733L;
	public String filename;
	public ConcurrentHashMap<Integer, HDFSBlock> blocks;

	public ConcurrentHashMap<Integer, HDFSBlock> getBlockList() {
		return blocks;
	}

	public int getBlockSize() {
		return blocks.size();
	}

	public void addBlock(Byte[] data, int blockID, int blocksize,
			List<String> locations) {

		HDFSBlock block = new HDFSBlock(this.filename, blockID, data,
				blocksize, locations);

		this.blocks.put(blockID, block);

	}

	public int getBlock(byte[] data, int blockID) {

		
		return -1;
	}

	public HDFSFile(String filename) {
		this.filename = filename;
		this.blocks = new ConcurrentHashMap<Integer, HDFSBlock>();
	}
}
