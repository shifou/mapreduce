package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskTrackerRemoteInterface extends Remote {

	public void healthCheck(Boolean b) throws RemoteException;
	
}
