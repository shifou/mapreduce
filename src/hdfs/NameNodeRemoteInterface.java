package hdfs;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import data.Message;
public interface NameNodeRemoteInterface extends Remote{
	public String delete(String path) throws RemoteException, IOException;
	public String deleteR(String path) throws RemoteException, IOException;
	public String copyToLocal(String hdfsFilePath, String localFilePath) throws RemoteException;
	public String copyToLocalR(String hdfsFilePath, String localFilePath) throws RemoteException;
	public String copyFromLocal(String localFilePath, String hdfsFilePath) throws RemoteException;
	public String copyFromLocalR(String localFilePath, String hdfsFilePath) throws RemoteException;
	public String join(String ip) throws RemoteException;
	public String list() throws RemoteException;
	public String quit() throws RemoteException;
}
