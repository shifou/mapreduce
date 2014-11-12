package mapreduce;



import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;


public class JobTracker implements JobTrackerRemoteInterface {

	
	private JobTrackerRemoteInterface jobTrackerStub;
	private int taskTrackerAssignID;
	public ConcurrentHashMap<String, TaskTrackerInfo> taskTrackers;
	public ConcurrentHashMap<String,String> jobid2JarName;
	public JobTracker(){
		this.taskTrackerAssignID = 1;
		this.taskTrackers = new ConcurrentHashMap<String, TaskTrackerInfo>();
		jobid2JarName = new ConcurrentHashMap<String,String>();
	}
	
	public ConcurrentHashMap<String, TaskTrackerInfo> getTaskTrackers(){
		return this.taskTrackers;
	}
	
	public boolean start(){
		
		if (!Environment.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME) && !Environment.createDirectory(Environment.MapReduceInfo.JOBFOLDER)){
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
	public synchronized String putJar(String jobid, String jarname, Byte []arr, int ct) {
		try {
			if(Environment.createDirectory(Environment.MapReduceInfo.JOBFOLDER+"/"+jobid)==false)
				return "can not create jobid folder for jar\n";
			FileOutputStream out = new FileOutputStream(Environment.Dfs.DIRECTORY+"/"+Environment.MapReduceInfo.JOBFOLDER+"/"+jobid+"/"+jarname,true);
			byte[] buff = new byte[ct];
			jobid2JarName.put(jobid, jarname);
			for(int i=0;i<ct;i++)
				buff[i]=arr[i].byteValue();
			out.write(buff, 0, ct);
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return "jar not found";
		} catch (IOException e) {
			e.printStackTrace();
			return "output jar path not found";
		}
		return "put jar ok";
	}
	@Override
	public String join(String IP) throws RemoteException {
		String serviceName = "t" + this.taskTrackerAssignID;
		TaskTrackerInfo taskInfo = new TaskTrackerInfo(IP, serviceName, Environment.TIME_LIMIT);
		this.taskTrackers.put(serviceName, taskInfo);
		
		this.taskTrackerAssignID++;
		return serviceName;
		
	}

}
