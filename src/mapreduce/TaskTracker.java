package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import main.Environment;

public class TaskTracker implements TaskTrackerRemoteInterface {
	public int curSlots;
	public String serviceName;
	public int partionNum;
	private JobTrackerRemoteInterface jobTrackerStub;
	private TaskTrackerRemoteInterface taskTrackerStub;
	public static ExecutorService threadPool;
	public TaskTracker(){
		curSlots= Environment.MapReduceInfo.SLOTS;
		threadPool = Executors.newFixedThreadPool(curSlots);
	}
	
	public boolean start(){
		
		
		try {
			
			Registry reg = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_IP, Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			this.jobTrackerStub = (JobTrackerRemoteInterface)reg.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
			this.serviceName = this.jobTrackerStub.join(InetAddress.getLocalHost().getHostAddress());
			if (!Environment.createDirectory(this.serviceName)){
				return false;
			}
			this.taskTrackerStub = (TaskTrackerRemoteInterface)UnicastRemoteObject.exportObject(this, 0);
			Registry r = LocateRegistry.getRegistry(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			r.rebind(this.serviceName, this.taskTrackerStub);
			
		} catch (RemoteException | NotBoundException | UnknownHostException e) {
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
    public void report(TaskInfo info)
    {
    	jobTrackerStub.getReport(info);
    }
    public String runTask(Task tk) throws RemoteException
    {
    	if(tk.locality)
    	{
    		
    	}
    	else
    	{
    		return "running";
    	}
    	if(tk.getType().equals(Task.TaskType.Mapper))
    	{

			MapRunner mapRunner = new MapRunner(tk.jobid, tk.taskid, tk.getSplit(), tk.config,serviceName, tk.reduceNum);
			threadPool.execute(mapRunner);
			return "running";
    	}
    	else
    	{

    		ReduceRunner reduceRunner = new ReduceRunner(tk.jobid, tk.taskid, tk.getSplit(), tk.config,serviceName);
    		threadPool.execute(reduceRunner);
			return "running";
    	}
    }
	@Override
	public void healthCheck(Boolean b) throws RemoteException {
	
		
	}
	
}
