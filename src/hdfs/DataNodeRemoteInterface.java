package hdfs;

import java.rmi.Remote;

public interface DataNodeRemoteInterface extends Remote{

	public boolean delete(String path, int ID);
	public void putFile(Byte[] data, int blockSize, HDFSBlock block); 
	public Byte[] getFile(HDFSBlock block);
	
}
