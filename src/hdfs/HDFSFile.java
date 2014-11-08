package hdfs;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HDFSFile implements Serializable{

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
	@Override
	public String delete() {
		for (Integer one : blocks.keySet()) {
			HDFSBlock hold = blocks.get(one);
			try {
				if(hold.delete()==false)
					System.out.println("notice some nodes fail when delete the block");
			} catch (RemoteException e) {
				System.out.println("delete failed");
				System.exit(-1);
			} catch (NotBoundException e) {
				System.out.println("data node cant find");
				System.exit(-1);
			} catch (IOException e) {
				System.out.println("File Error");
				System.exit(-1);
			}
		}
		return filename;
	}
}
