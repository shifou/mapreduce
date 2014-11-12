package mapreduce;



import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;


public class JobTracker implements JobTrackerRemoteInterface {

	
	private JobTrackerRemoteInterface jobTrackerStub;
	private int taskTrackerAssignID;
	private ConcurrentHashMap<String, TaskTrackerInfo> taskTrackers;
	
	public JobTracker(){
		this.taskTrackerAssignID = 1;
		this.taskTrackers = new ConcurrentHashMap<String, TaskTrackerInfo>();
	}
	
	public ConcurrentHashMap<String, TaskTrackerInfo> getTaskTrackers(){
		return this.taskTrackers;
	}
	
	public boolean start(){
		
		if (!Environment.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME)){
			return false;
		}
		try {
			Registry registry = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			this.jobTrackerStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			registry.rebind(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME, this.jobTrackerStub);
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	
	@Override
	public String join(String IP) throws RemoteException {
		String serviceName = "t" + this.taskTrackerAssignID;
		TaskTrackerInfo taskInfo = new TaskTrackerInfo(IP, serviceName, Environment.TIME_LIMIT);
		this.taskTrackers.put(serviceName, taskInfo);
		
		this.taskTrackerAssignID++;
		return serviceName;
		
	}

	@Override
	public JobInfo submitJob(Job job) throws RemoteException {
		
		return null;
	}

	@Override
	public JobInfo getJobStatus(int ID) throws RemoteException {
		
		return null;
	}

}
