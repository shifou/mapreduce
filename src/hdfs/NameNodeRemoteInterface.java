package hdfs;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import data.Message;
public interface NameNodeRemoteInterface extends Remote{
	public Message heartBeat(int dataNodeId) throws RemoteException;
	public void delete(String path) throws RemoteException, IOException;
	
}
