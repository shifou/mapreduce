package mapreduce;



import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;


public class JobTracker implements JobTrackerRemoteInterface {

	public ConcurrentHashMap<String,Job> jobs;
	private JobTrackerRemoteInterface jobTrackerStub;
	private int taskTrackerAssignID;
	public int jobID;
	public ConcurrentHashMap<String, TaskTrackerInfo> taskTrackers;
	public ConcurrentHashMap<String,String> jobid2JarName;
	public JobTracker(){
		jobID=1;
		jobs= new ConcurrentHashMap<String,Job>();
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
	
	@Override
	public synchronized String putJar(String jobid, String jarname, Byte []arr, int ct) throws RemoteException{
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

	@Override
	public synchronized JobInfo submitJob(Job job) throws RemoteException {
		String jobid = String.format("%d", new Date().getTime())+"_"+this.jobID;
		jobs.put(jobid, job);
		jobID++;
		JobInfo ans = new JobInfo(jobid);
		allocateMapTask(job);
		return ans;
	}

	public void allocateMapTask(Job job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public JobInfo getJobStatus(String ID) throws RemoteException {
		
		return null;
	}

	@Override
	public Byte[] getJar(String jobid, long pos)throws RemoteException {
		if(jobid2JarName.containsKey(jobid)==false)
			return null;
		String name = this.jobid2JarName.get(jobid);
		try {
			RandomAccessFile raf = new RandomAccessFile(Environment.MapReduceInfo.JOBFOLDER+"/"+jobid+"/"+name, "r");
			raf.seek(pos);
			Byte []ans= new Byte[(int) Math.min(Environment.Dfs.BUF_SIZE, raf.length()-pos)];
			for(int i=0;i<ans.length;i++)
				ans[i]=raf.readByte();
			raf.close();
			return ans;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	private void assignTasks(){
		
	}

}
