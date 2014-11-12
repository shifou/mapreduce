package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface JobTrackerRemoteInterface extends Remote {

	public String join(String IP) throws RemoteException;
	public String putJar(String jobid, String jarname, Byte []arr, int ct) throws RemoteException;
	public String getJar(String jobid)
}
