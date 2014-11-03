package hdfs;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import data.Message;
public interface NameNodeRemoteInterface extends Remote{
	public Message heartBeat(int dataNodeId) throws RemoteException;
	public String delete(String path) throws RemoteException, IOException;
	public String copyToLocal(String hdfsFilePath, String localFilePath);
	public String copyFromLocal(String localFilePath, String hdfsFilePath);
	public String listFiles();
	
}
