package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{

	public boolean delete(String path, int ID) throws RemoteException;
	public void putFile(Byte[] data, int blockSize, HDFSBlock block) throws RemoteException; 
	public Byte[] getFile(HDFSBlock block) throws RemoteException;
	public String deleteFolder(String foldername);
	
}
