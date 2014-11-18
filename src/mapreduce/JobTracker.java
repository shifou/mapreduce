package mapreduce;




import hdfs.NameNodeRemoteInterface;







import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import main.Environment;


public class JobTracker implements JobTrackerRemoteInterface {

	public ConcurrentHashMap<String,Job> jobs;
	private JobTrackerRemoteInterface jobTrackerStub;
	private int taskTrackerAssignID;
	public int jobID;
	public ConcurrentHashMap<String, TaskTrackerInfo> taskTrackers;
	public ConcurrentHashMap<String,String> jobid2JarName;
	private ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>> jobToMappers; //JobID -> Map of Task -> TaskTrackerInfo 
	private ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>> jobToReducers;
	private ConcurrentLinkedQueue<Job> queuedJobs;
	private Job currentJob;
	public JobTracker(){
		this.jobID=1;
		this.jobs= new ConcurrentHashMap<String,Job>();
		this.taskTrackerAssignID = 1;
		this.taskTrackers = new ConcurrentHashMap<String, TaskTrackerInfo>();
		this.jobid2JarName = new ConcurrentHashMap<String,String>();
		this.jobToMappers = new ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>>();
		this.jobToReducers = new ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>>();
		this.queuedJobs = new ConcurrentLinkedQueue<Job>();
		this.currentJob = null;
	}
	
	public ConcurrentHashMap<String, TaskTrackerInfo> getTaskTrackers(){
		return this.taskTrackers;
	}
	
	public boolean start(){
		
		if (!Environment.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME) ){
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
			if(Environment.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME+"/"+jobid)==false)
				return "can not create jobid folder for jar\n";
			FileOutputStream out = new FileOutputStream(Environment.Dfs.DIRECTORY+"/"+Environment.MapReduceInfo.JOBTRACKER_SERVICENAME+"/"+jobid+"/"+jarname,true);
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
		TaskTrackerInfo taskInfo = new TaskTrackerInfo(IP, serviceName, Environment.TIME_LIMIT, this.taskTrackerAssignID);
		this.taskTrackers.put(serviceName, taskInfo);
		
		this.taskTrackerAssignID++;
		return serviceName;
		
	}

	@Override
	public synchronized JobInfo submitJob(Job job) throws RemoteException {
		String jobid = String.format("%d", new Date().getTime())+"_"+this.jobID;
		jobs.put(jobid, job);
		jobID++;
		JobInfo info = new JobInfo(jobid);
		job.info = info;
		if (this.currentJob == null){
			jobStart(job);
		}
		else {
			this.queuedJobs.offer(job);
		}
		return info;
	}
	
	private  void jobStart(Job job){
		try {
			Registry r = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNode = (NameNodeRemoteInterface)r.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			InputSplit[] splits = nameNode.getSplit(job.getInputPath());
			job.info.setNumMappers(splits.length);
			for (int i = 0; i < splits.length; i++){
				Task task = new Task(job.getJarClass(), Task.TaskType.Mapper, job.conf);
				task.setSplit(splits[i]);
				allocateMapTask(job, task);
			}
			
			
		} catch (NotBoundException | RemoteException e) {
			
			e.printStackTrace();
		}
	}

	
	private void allocateMapTask(Job j, Task t){
		HashSet<Integer> locations = t.getSplit().getLocations();
		String bestNode = null;
		int bestLoad = Environment.MapReduceInfo.SLOTS;
		t.locality = true;
		for (int i : locations){
			String taskTrackerName = "t"+i;
			if ((this.taskTrackers.get(taskTrackerName).slotsFilled < Environment.MapReduceInfo.SLOTS) && (this.taskTrackers.get(taskTrackerName).slotsFilled < bestLoad)){
				bestNode = taskTrackerName;
				bestLoad = this.taskTrackers.get(taskTrackerName).slotsFilled;
			}
		}
		if (bestNode == null){
			for (String s : this.taskTrackers.keySet()){
				TaskTrackerInfo i = this.taskTrackers.get(s);
				if ((i.slotsFilled < bestLoad) && (!locations.contains(i.slaveNum))){
					bestLoad = i.slotsFilled;
					bestNode = s;
					t.locality = false;
				}
			}
		}
		if (this.jobToMappers.get(j.info.getID()) != null){
				this.taskTrackers.get(bestNode).slotsFilled += 1;
				this.jobToMappers.get(j.info.getID()).put(t, this.taskTrackers.get(bestNode));
		}
		else {
			ConcurrentHashMap<Task, TaskTrackerInfo> temp = new ConcurrentHashMap<Task, TaskTrackerInfo>();
			this.taskTrackers.get(bestNode).slotsFilled+=1;
			temp.put(t, this.taskTrackers.get(bestNode));
			this.jobToMappers.put(j.info.getID(), temp);
		}
		if (bestNode == null){
			//Not enough slots to run all maps 
			//What should I do? 
		}
		else {
			Registry r;
			try {
				r = LocateRegistry.getRegistry(this.taskTrackers.get(bestNode).IP, Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface)r.lookup(bestNode);
				taskTracker.runTask(t);
			} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
			}
			
		}
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
			RandomAccessFile raf = new RandomAccessFile(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME+"/"+jobid+"/"+name, "r");
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

	@Override
	public void getReport(TaskInfo info) {
		// TODO Auto-generated method stub
		
	}


}
