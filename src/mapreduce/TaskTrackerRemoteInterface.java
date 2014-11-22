package mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Vector;

import mapreduce.io.Record;
import mapreduce.io.Writable;

public interface TaskTrackerRemoteInterface extends Remote {

	public void healthCheck(Boolean b) throws RemoteException;
	public String runTask(Task tk) throws RemoteException;
	public Vector<Record<Writable, Writable>> getPartition(String jobid, Integer maptaskid,
			String taskid);
}
