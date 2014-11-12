package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobTrackerRemoteInterface extends Remote {

	public String join(String IP) throws RemoteException;
	public JobInfo submitJob(Job job) throws RemoteException;
	public JobInfo getJobStatus(int ID) throws RemoteException;
	
}
