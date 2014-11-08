package hdfs;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import data.Message;
public interface NameNodeRemoteInterface extends Remote{
	public String delete(String path) throws RemoteException, IOException;
	public String deleteR(String path) throws RemoteException, IOException;
	public String copyToLocal(String hdfsFilePath, String localFilePath);
	public String copyToLocalR(String hdfsFilePath, String localFilePath);
	public String copyFromLocal(String localFilePath, String hdfsFilePath);
	public String copyFromLocalR(String localFilePath, String hdfsFilePath);
	public String join(String ip);
	public String list();
	public String heart(int id);
	public void quit();
}
