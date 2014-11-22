package mapreduce;

import hdfs.NameNodeRemoteInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import main.Environment;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.Writable;

public class TaskTracker implements TaskTrackerRemoteInterface {
	public int curSlots;
	public String serviceName;
	public int partionNum;
	public HashMap<String, HashMap<String,Thread>> mapTasks;
	public HashMap<String, HashMap<String,Thread>> reduceTasks;
	private static JobTrackerRemoteInterface jobTrackerStub;
	private TaskTrackerRemoteInterface taskTrackerStub;
	public static ExecutorService threadPool;
	public TaskTracker(){
		curSlots= Environment.MapReduceInfo.SLOTS;
		threadPool = Executors.newCachedThreadPool();
	}
	// HDFS
		// |---tasktrackerServiceName
		//     |----jobid
		//			|----mapper
		//				|----taskid(1-Block_size)_partition_id(1-tasktracker size)
		//			|----reducer
		//				|----taskid(1-Slave_size)
		//			xxx.jar
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
    public static void report(TaskInfo info)
    {
    	jobTrackerStub.getReport(info);
    }
    public String runTask(Task tk) throws RemoteException
    {
    	String jarpath="";
    	String path=Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+tk.jobid;
    	File tt= new File(path);
    	if(tt.exists()==false)
    		tt.mkdir();
		try {
    		Registry jobRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.MapReduceInfo.JOBTRACKER_PORT);
    		JobTrackerRemoteInterface jobStub;

			jobStub = (JobTrackerRemoteInterface) jobRegistry
						.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
			
			int pos=0;
			File jar = new File(path+"/"+tk.config.jarName);
			jarpath= path+"/"+tk.config.jarName;
			if(jar.exists()==false)
			{
			FileOutputStream op = new  FileOutputStream(jarpath,true);
			while(true)
			{
				Byte[] ans = jobStub.getJar(tk.jobid, pos);
				if(ans==null)
					return "jar not exist";
				else if(ans.length==0)
					break;
				else
				{
					byte[]data= new byte[ans.length];
					for(int i=0;i<ans.length;i++)
						data[i]=ans[i].byteValue();
					op.write(data);
					op.flush();
					op.close();
				}
			}
			}
		}
		 catch (NotBoundException e) {
				// TODO Auto-generated catch block
			 e.printStackTrace();
				return "error";
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "file open error";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "file write error";
		}
		
    	if(tk.getType().equals(Task.TaskType.Mapper))
    	{

			MapRunner mapRunner = new MapRunner(tk.jobid, tk.taskid, tk.getSplit(), tk.config,serviceName, tk.reduceNum,jarpath,true);
			 Thread oneThread = new Thread(mapRunner);
			 oneThread.stop();
			threadPool.execute(mapRunner);
			return "running";
    	}
    	else
    	{

    		ReduceRunner reduceRunner = new ReduceRunner(tk.jobid, tk.taskid, tk.mploc, tk.config,serviceName,jarpath);
    		threadPool.execute(reduceRunner);
			return "running";
    	}
    }
	@Override
	public void healthCheck(Boolean b) throws RemoteException {
	
		
	}
	// HDFS
	// |---tasktrackerServiceName
	//     |----jobid
	//			|----mapper
	//				|----taskid(1-Block_size)_partition_id(1-tasktracker size)
	//			|----reducer
	//				|----taskid(1-Slave_size)
	//			xxx.jar
	@Override
	public Vector<Record<Text,Text>> getPartition(String jobid, Integer maptaskid,
			String taskid) {
		String path=Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+jobid+"/mapper/"+maptaskid+"_"+taskid;
		System.out.println("get partition from local: "+path);
		File a =new File(path);
		if(a.exists()==false)
			return null;
		String line="";
		BufferedReader reader;
		Vector<Record<Text,Text>> ans= new Vector<Record<Text,Text>>();
		try {
			reader = new BufferedReader(new FileReader(a));
			while ((line = reader.readLine()) != null) {
				String []tt=line.split("\t");
				Record<Text,Text> inp =new Record<Text,Text>(new Text(tt[0]),new Text(tt[1]));
				ans.add(inp);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		return ans;
	}
	@Override
	public boolean killFaildJob(String jobid) throws RemoteException {
		boolean flag=false;
		if(mapTasks.containsKey(jobid)==true)
		{
			for(String one: mapTasks.get(jobid).keySet())
			{
				Thread hold = mapTasks.get(jobid).get(one);
				hold.stop();
			}
			flag=true;
		}
		if(reduceTasks.containsKey(jobid)==true)
		{
			for(String one: reduceTasks.get(jobid).keySet())
			{
				Thread hold = reduceTasks.get(jobid).get(one);
				hold.stop();
			}
			flag=true;
		}
		if(flag==false)
			return false;
		else
			return true;
	}
	
}
