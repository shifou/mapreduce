package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import main.Environment;

public class TaskTracker implements TaskTrackerRemoteInterface {
	public int curSlots;
	private String serviceName;
	private JobTrackerRemoteInterface jobTrackerStub;
	private TaskTrackerRemoteInterface taskTrackerStub;
	
	public TaskTracker(){
		
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

	@Override
	public void healthCheck(Boolean b) throws RemoteException {
	
		
	}
	
}
