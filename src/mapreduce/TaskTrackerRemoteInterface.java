package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Vector;

import mapreduce.io.Record;

public interface TaskTrackerRemoteInterface extends Remote {

	public void healthCheck(Boolean b) throws RemoteException;
	public String runTask(Task tk) throws RemoteException;
	public boolean killFaildJob(String jobid) throws RemoteException;
	public Vector<Record> getPartition(String jobid, Integer maptaskid,
			String taskid) throws RemoteException;
	public boolean uplodaToHDFS(String jobid) throws RemoteException;
}
