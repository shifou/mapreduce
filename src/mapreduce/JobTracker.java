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
import mapreduce.Task.TaskType;

public class JobTracker implements JobTrackerRemoteInterface {

	public ConcurrentHashMap<String, Job> jobs;
	private JobTrackerRemoteInterface jobTrackerStub;
	private int taskTrackerAssignID;
	public int globalJobID;
	public static ConcurrentHashMap<String, TaskTrackerInfo> taskTrackers;
	public ConcurrentHashMap<String, String> jobid2JarName;
	private ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>> jobToMappers; // JobID  -> Map of Task -> TaskTrackerInfo
	private ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>> jobToReducers;
	private ConcurrentLinkedQueue<Job> queuedJobs;
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<Task>> queuedMapTasks;
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<Task>> queuedReduceTasks;
	
	private ConcurrentHashMap<String, HashSet<TaskInfo>> completedMaps;
	private ConcurrentHashMap<String, HashSet<String>> jobToTaskTrackers;
	private ConcurrentHashMap<String, HashSet<Task>> taskTrackerToTasks;
	private int MapSlots;
	private int ReduceSlots;

	public JobTracker() {
		this.globalJobID = 1;
		this.jobs = new ConcurrentHashMap<String, Job>();
		this.taskTrackerAssignID = 1;
		JobTracker.taskTrackers = new ConcurrentHashMap<String, TaskTrackerInfo>();
		this.jobid2JarName = new ConcurrentHashMap<String, String>();
		this.jobToMappers = new ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>>();
		this.jobToReducers = new ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>>();
		this.queuedJobs = new ConcurrentLinkedQueue<Job>();
		this.queuedMapTasks = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Task>>();
		this.queuedReduceTasks = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Task>>();
		this.completedMaps = new ConcurrentHashMap<String, HashSet<TaskInfo>>();
		this.jobToTaskTrackers = new ConcurrentHashMap<String, HashSet<String>>();
		this.taskTrackerToTasks = new ConcurrentHashMap<String, HashSet<Task>>();
		this.ReduceSlots = Environment.MapReduceInfo.SLOTS/2;
		this.MapSlots = Environment.MapReduceInfo.SLOTS - this.ReduceSlots;
	}

	public ConcurrentHashMap<String, TaskTrackerInfo> getTaskTrackers() {
		return JobTracker.taskTrackers;
	}

	public boolean start() {

		if (!Environment
				.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME)) {
			return false;
		}
		try {
			Registry registry = LocateRegistry
					.createRegistry(Environment.MapReduceInfo.JOBTRACKER_PORT);
			this.jobTrackerStub = (JobTrackerRemoteInterface) UnicastRemoteObject
					.exportObject(this, 0);
			registry.rebind(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME,
					this.jobTrackerStub);
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		}
		return true;

	}

	@Override
	public synchronized String putJar(String jobid, String jarname, Byte[] arr,
			int ct) throws RemoteException {
		try {
			FileOutputStream out = new FileOutputStream(
					Environment.Dfs.DIRECTORY + "/"
							+ Environment.MapReduceInfo.JOBTRACKER_SERVICENAME
							+ "/" + jobid + "/" + jarname, true);
			byte[] buff = new byte[ct];
				//System.out.println("put jar: "+jarname+" for job "+jobid);
			for (int i = 0; i < ct; i++)
				buff[i] = arr[i].byteValue();
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
	public synchronized Byte[] getJar(String jobid, long pos) throws RemoteException {
		if (jobid2JarName.containsKey(jobid) == false)
		{
			System.out.println("??????????");
			return null;
		
		}
		String name = this.jobid2JarName.get(jobid);
		try {
			RandomAccessFile raf = new RandomAccessFile(
					Environment.Dfs.DIRECTORY + "/"
							+ Environment.MapReduceInfo.JOBTRACKER_SERVICENAME + "/"
							+ jobid + "/" + name, "r");
			System.out.println("#jobtracker open# "+Environment.MapReduceInfo.JOBTRACKER_SERVICENAME + "/"
					+ jobid + "/" + name);
			raf.seek(pos);
			Byte[] ans = new Byte[(int) Math.min(Environment.Dfs.BUF_SIZE,
					raf.length() - pos)];
			for (int i = 0; i < ans.length; i++)
				ans[i] = raf.readByte();
			raf.close();
			return ans;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("get jar error!");
			return null;
		}

	}

	@Override
	public synchronized String join(String IP) throws RemoteException {
		String serviceName = "t" + this.taskTrackerAssignID;
		TaskTrackerInfo taskInfo = new TaskTrackerInfo(IP, serviceName,
				Environment.TIME_LIMIT, this.taskTrackerAssignID);
		JobTracker.taskTrackers.put(serviceName, taskInfo);

		this.taskTrackerAssignID++;
		return serviceName;

	}
	
	public String getJobID(Job job) throws RemoteException {
		String jobid = String.format("%d", new Date().getTime()) + "_"
				+ this.globalJobID;
		jobid2JarName.put(jobid, job.conf.jarName);
		Environment.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME+ "/" + jobid);
		globalJobID++;
		JobInfo info = new JobInfo(jobid);
		job.info = info;
		jobs.put(jobid, job);
		return jobid;
	}

	public JobInfo submitJob(String id) throws RemoteException {
		Job job = this.jobs.get(id);
		System.out.println("In submitJob!");
		boolean startJob = false;
		System.out.println("start job?");
		for(String taskTracker: taskTrackers.keySet()){
			TaskTrackerInfo tInfo = taskTrackers.get(taskTracker);
			if (tInfo.mapSlotsFilled < this.MapSlots){
				startJob = true;
				break;
			}
		}
		if (startJob){
			System.out.println("start job: yes");
			jobStart(job);
		}
		else {
			System.out.println("queue job!");
			job.info.setStatus(JobInfo.WAITING);
			this.queuedJobs.offer(job);
		}
		
		return job.info;
	}

	private void jobStart(Job job) {
		System.out.println("Starting job: " + job.info.getID()+"...");
		try {
			Registry r = LocateRegistry
					.getRegistry(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNode = (NameNodeRemoteInterface) r
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			InputSplit[] splits = nameNode.getSplit(job.conf.getInputPath());
			int len = splits.length;
			job.info.setNumMappers(len);
			for (int i = 0; i < len; i++) {
				Task task = new Task( Task.TaskType.Mapper,
						job.conf, null);
				task.setSplit(splits[i]);
				task.jobid = job.info.getID();
				task.taskid = "" + splits[i].getBlock().getID();
				if (len < taskTrackers.size()){
					task.reduceNum = len;
				}
				else {
					task.reduceNum = taskTrackers.size();
				}
				allocateMapTask(task);
				System.out.println("set size: "+task.reduceNum);
			}
			job.info.setStatus(JobInfo.RUNNING);

		} catch (NotBoundException | RemoteException e) {

			e.printStackTrace();
		}
	}

	private void allocateMapTask(Task t) {
		System.out.println("Allocating mapTask: taskId" + t.taskid+" JobID: " + t.jobid);
		HashSet<Integer> locations = t.getSplit().getLocations();
		String bestNode = null;
		int bestLoad = this.MapSlots;
		t.locality = true;
		System.out.println("1");
		for (int i : locations) {
			String taskTrackerName = "t" + i;
			if ((JobTracker.taskTrackers.get(taskTrackerName).mapSlotsFilled < this.MapSlots)
					&& (JobTracker.taskTrackers.get(taskTrackerName).mapSlotsFilled < bestLoad)) {
				bestNode = taskTrackerName;
				bestLoad = JobTracker.taskTrackers.get(taskTrackerName).mapSlotsFilled;
			}
		}
		System.out.println("2");
		if (bestNode == null) {
			for (String s : JobTracker.taskTrackers.keySet()) {
				TaskTrackerInfo i = JobTracker.taskTrackers.get(s);
				if ((i.mapSlotsFilled < bestLoad)
						&& (!locations.contains(i.slaveNum))) {
					bestLoad = i.mapSlotsFilled;
					bestNode = s;
					t.locality = false;
				}
			}
		}
		if (this.jobToMappers.get(t.jobid) != null) {
			if (bestNode != null){
				JobTracker.taskTrackers.get(bestNode).mapSlotsFilled += 1;
				this.jobToMappers.get(t.jobid).put(t,
						JobTracker.taskTrackers.get(bestNode));
			}
		} else {
			if (bestNode != null){
				ConcurrentHashMap<Task, TaskTrackerInfo> temp = new ConcurrentHashMap<Task, TaskTrackerInfo>();
				JobTracker.taskTrackers.get(bestNode).mapSlotsFilled += 1;
				temp.put(t, JobTracker.taskTrackers.get(bestNode));
				this.jobToMappers.put(t.jobid, temp);
			}
			
		}
		if (bestNode == null) {
			if (this.queuedMapTasks.get(t.jobid) == null){
				this.queuedMapTasks.put(t.jobid, new ConcurrentLinkedQueue<Task>());
			}
			this.queuedMapTasks.get(t.jobid).add(t);
		} else {
			Registry r;
			try {
				r = LocateRegistry.getRegistry(
						JobTracker.taskTrackers.get(bestNode).IP,
						Environment.MapReduceInfo.TASKTRACKER_PORT);
				if (this.taskTrackerToTasks.get(bestNode) == null){
					this.taskTrackerToTasks.put(bestNode, new HashSet<Task>());
				}
				this.taskTrackerToTasks.get(bestNode).add(t);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface) r
						.lookup(bestNode);
				System.out.println(taskTracker.runTask(t));
				
			} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public synchronized JobInfo getJobStatus(String ID) throws RemoteException {
		return this.jobs.get(ID).info;
	}

	
	@Override
	public void getReport(TaskInfo info) throws RemoteException {
		System.out.println("REPORT RECVD!");
		TaskStatus status = info.st;
		Task.TaskType type = info.type;
		if (status == TaskStatus.FINISHED) {
			if (type == TaskType.Mapper) {
				for (Task t : this.jobToMappers.get(info.jobid).keySet()) {
					if (t.taskid.equals(info.taskid)) {
						System.out.println("MapTask completed: TaskID: "+ info.taskid + " JobID: " + info.jobid);
						String tracker = this.jobToMappers.get(info.jobid).get(
								t).serviceName;
						JobTracker.taskTrackers.get(tracker).mapSlotsFilled = Math
								.max(0,
										JobTracker.taskTrackers.get(tracker).mapSlotsFilled - 1);
						this.jobToMappers.get(info.jobid).remove(t);
						if (this.jobToTaskTrackers.get(info.jobid) == null) {
							this.jobToTaskTrackers.put(info.jobid,
									new HashSet<String>());
						}
						this.jobToTaskTrackers.get(info.jobid).add(info.who);
						this.jobs.get(info.jobid).info.incrementComplMappers();
						HashSet<TaskInfo> completed = this.completedMaps
								.get(info.jobid);
						if (completed == null) {
							completed = new HashSet<TaskInfo>();
						}
						completed.add(info);
						this.taskTrackerToTasks.get(tracker).remove(t);
						this.completedMaps.put(info.jobid, completed);
						if (this.jobs.get(info.jobid).info
								.getPercentMapCompleted() == 100) {
							System.out.println("All maps completed: " + info.jobid);
							startReduceForJob(this.jobs.get(info.jobid));
						} else {
							ConcurrentLinkedQueue<Task> queuedMaps = this.queuedMapTasks.get(info.jobid);
							if (queuedMaps != null){
								allocateMapTask(queuedMaps.poll());
								if (queuedMaps.isEmpty()){
									this.queuedMapTasks.remove(info.jobid);
								}
							}
						}
					}
				}
			} else if (type == TaskType.Reducer) {
				System.out.println("ReduceTask completed: TaskID: "+ info.taskid + " JobID: " + info.jobid);
				for (Task t : this.jobToReducers.get(info.jobid).keySet()) {
					if (t.taskid.equals(info.taskid)) {
						String tracker = this.jobToReducers.get(info.jobid)
								.get(t).serviceName;
						JobTracker.taskTrackers.get(tracker).reduceSlotsFilled = Math
								.max(0,
										JobTracker.taskTrackers.get(tracker).reduceSlotsFilled - 1);
						this.jobs.get(info.jobid).info.incrementComplReducers();
						this.taskTrackerToTasks.get(tracker).remove(t);
						if (this.jobs.get(info.jobid).info
								.getPercentReduceCompleted() == 100) {
							commit(info.jobid);
							this.jobs.get(info.jobid).info.setStatus(JobInfo.SUCCEEDED);
							this.jobToMappers.remove(info.jobid);
							this.jobToReducers.remove(info.jobid);
							this.completedMaps.remove(info.jobid);
							this.jobToTaskTrackers.remove(info.jobid);
							if (!this.queuedJobs.isEmpty()){
								jobStart(this.queuedJobs.poll());
							}
						}
						else {
							ConcurrentLinkedQueue<Task> queuedReduces = this.queuedReduceTasks.get(info.jobid);
							if (queuedReduces != null){
								allocateReduceTask(info.jobid, queuedReduces.poll(), info.who);
								if (queuedReduces.isEmpty()){
									this.queuedReduceTasks.remove(info.jobid);
								}
							}
						}
					}
				}
			}
		} else if (status == TaskStatus.FAILED) {
			if (this.jobs.containsKey(info.jobid) && (this.jobs.get(info.jobid).info.getStatus() != JobInfo.FAILED)){
				ConcurrentHashMap<Task, TaskTrackerInfo> temp = this.jobToMappers.get(info.jobid);
				HashSet<TaskTrackerInfo> trackers = new HashSet<TaskTrackerInfo>();
				for (Task t: temp.keySet()){
					trackers.add(temp.get(t));
				}
				for (TaskTrackerInfo tInfo: trackers){
					String tracker = tInfo.serviceName;
					Registry reg;
					try {
						reg = LocateRegistry.getRegistry(tInfo.IP, Environment.MapReduceInfo.TASKTRACKER_PORT);
						TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface)reg.lookup(tracker);
						taskTracker.killFaildJob(info.jobid);
					} catch (RemoteException | NotBoundException e) {
						e.printStackTrace();
					}
				}
				if (this.jobToMappers.get(info.jobid) != null){
					for (Task t: this.jobToMappers.get(info.jobid).keySet()){
						if (t.jobid.equals(info.jobid)){
							taskTrackers.get(this.jobToMappers.get(info.jobid).get(t).serviceName).mapSlotsFilled = Math.max(0, taskTrackers.get(this.jobToMappers.get(info.jobid).get(t).serviceName).mapSlotsFilled-1);
							if (this.taskTrackerToTasks.containsKey(this.jobToMappers.get(info.jobid).get(t).serviceName)){
								this.taskTrackerToTasks.get(this.jobToMappers.get(info.jobid).get(t).serviceName).remove(t);
							}
						}
					}
					this.jobToMappers.remove(info.jobid);
				}
				
				if (this.jobToReducers.get(info.jobid) != null){
					for (Task t: this.jobToReducers.get(info.jobid).keySet()){
						if (t.jobid.equals(info.jobid)){
							taskTrackers.get(this.jobToReducers.get(info.jobid).get(t).serviceName).reduceSlotsFilled = Math.max(0, taskTrackers.get(this.jobToReducers.get(info.jobid).get(t).serviceName).reduceSlotsFilled-1);
							if (this.taskTrackerToTasks.containsKey(this.jobToReducers.get(info.jobid).get(t).serviceName)){
								this.taskTrackerToTasks.get(this.jobToReducers.get(info.jobid).get(t).serviceName).remove(t);
							}
						}
					}
					this.jobToReducers.remove(info.jobid);
				}
				this.queuedMapTasks.remove(info.jobid);
				this.queuedReduceTasks.remove(info.jobid);
				this.jobs.get(info.jobid).info.setStatus(JobInfo.FAILED);
			}
			
		}

	}

	public String findIp(String name) {
		if (taskTrackers.containsKey(name)) {
			return taskTrackers.get(name).IP;
		} else
			return "";
	}

	private void startReduceForJob(Job job) {
		HashSet<String> trackers = this.jobToTaskTrackers.get(job.info.getID());
		int i = 0;
		for (String tracker : trackers) {
			Task t = createReduceTask(i, job);
			i += 1;
			allocateReduceTask(job.info.getID(), t, tracker);
		}
		job.info.setNumReducers(i);
	}

	private void allocateReduceTask(String jobID, Task t, String taskTrackerName) {
		System.out.println("Allocating reduceTask: taskId" + t.taskid+" JobID: " + t.jobid);
		if (JobTracker.taskTrackers.get(taskTrackerName).reduceSlotsFilled < this.ReduceSlots){
			if (this.jobToReducers.get(jobID) != null) {
				JobTracker.taskTrackers.get(taskTrackerName).reduceSlotsFilled += 1;
				this.jobToReducers.get(jobID).put(t,
						JobTracker.taskTrackers.get(taskTrackerName));
			} else {
				ConcurrentHashMap<Task, TaskTrackerInfo> temp = new ConcurrentHashMap<Task, TaskTrackerInfo>();
				JobTracker.taskTrackers.get(taskTrackerName).reduceSlotsFilled += 1;
				temp.put(t, JobTracker.taskTrackers.get(taskTrackerName));
				this.jobToReducers.put(jobID, temp);
			}
			Registry r;
			try {
				r = LocateRegistry.getRegistry(
						JobTracker.taskTrackers.get(taskTrackerName).IP,
						Environment.MapReduceInfo.TASKTRACKER_PORT);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface) r
						.lookup(taskTrackerName);
				this.taskTrackerToTasks.get(taskTrackerName).add(t);
				System.out.println(taskTracker.runTask(t));
				
			} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
			}
		}
		else {
			if (this.queuedReduceTasks.get(jobID) == null){
				this.queuedReduceTasks.put(jobID, new ConcurrentLinkedQueue<Task>());
			}
			this.queuedReduceTasks.get(jobID).add(t);
		}
		
	}

	private Task createReduceTask(int partitionNum, Job job){
		HashSet<TaskInfo> tInfos = this.completedMaps.get(job.info.getID());
		ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> reduceMap = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>>();
		for (TaskInfo tInfo : tInfos){
			if (reduceMap.get(tInfo.who) == null){
				reduceMap.put(tInfo.who, new ConcurrentHashMap<Integer, String>());
			}
			reduceMap.get(tInfo.who).put(Integer.parseInt(tInfo.taskid), tInfo.mplocations.get(partitionNum));
		}
		Task t = new Task(TaskType.Reducer, job.conf, reduceMap);
		t.jobid = job.info.getID();
		t.taskid = "" + partitionNum;
		return t;
		
	}
	
	public void handleNodeFailure(String taskTrackerName){
		System.out.println("TaskTracker failure: "+taskTrackerName + "... Rescheduling lost tasks...");
		taskTrackers.remove(taskTrackerName);
		if (this.taskTrackerToTasks.get(taskTrackerName) != null){
			for (Task t: this.taskTrackerToTasks.get(taskTrackerName)){
				this.jobToTaskTrackers.get(t.jobid).remove(taskTrackerName);
				if (t.getType() == TaskType.Mapper){
					allocateMapTask(t);
				}
				else if (t.getType() == TaskType.Reducer){
					String bestNode = null;
					int bestLoad = this.ReduceSlots;
					for (String tracker : this.jobToTaskTrackers.get(t.jobid)){
						if (taskTrackers.get(tracker).reduceSlotsFilled < bestLoad){
							bestLoad = taskTrackers.get(tracker).reduceSlotsFilled;
							bestNode = tracker;
						}
					}
					if (bestNode != null){
						allocateReduceTask(t.jobid, t, bestNode);
					}
					else {
						if (this.queuedReduceTasks.get(t.jobid) == null){
							this.queuedReduceTasks.put(t.jobid, new ConcurrentLinkedQueue<Task>());
						}
						this.queuedReduceTasks.get(t.jobid).add(t);
					}
				}
			}
			this.taskTrackerToTasks.remove(taskTrackerName);
		}
		else {
			System.out.println("But nothing to reschedule!");
		}
		
	}

	@Override
	public synchronized String listjob() throws RemoteException {
		String ret = "";
		for (String jobID : this.jobs.keySet()){
			Job job = this.jobs.get(jobID);
			String mapName = job.conf.getMapperClass();
			String reduceName = job.conf.getReducerClass();
			String inputPath = job.conf.getInputPath();
			String outputPath = job.conf.getOutputPath();
			String jarName = job.conf.getJarName();
			String temp = jobID + " " + mapName + " " + " " + reduceName + " " + inputPath + " "+outputPath+" "+jarName+"\n";
			ret = ret + temp;
		}
		return ret;
	}

	@Override
	public synchronized String killJob(String jobid) throws RemoteException {
		if (this.jobs.containsKey(jobid) && (this.jobs.get(jobid).info.getStatus() != JobInfo.FAILED)){
			ConcurrentHashMap<Task, TaskTrackerInfo> temp = this.jobToMappers.get(jobid);
			HashSet<TaskTrackerInfo> trackers = new HashSet<TaskTrackerInfo>();
			for (Task t: temp.keySet()){
				trackers.add(temp.get(t));
			}
			for (TaskTrackerInfo tInfo: trackers){
				String tracker = tInfo.serviceName;
				Registry reg;
				try {
					reg = LocateRegistry.getRegistry(tInfo.IP, Environment.MapReduceInfo.TASKTRACKER_PORT);
					TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface)reg.lookup(tracker);
					taskTracker.killFaildJob(jobid);
				} catch (RemoteException | NotBoundException e) {
					e.printStackTrace();
				}
			}
			if (this.jobToMappers.get(jobid)!=null){
				for (Task t: this.jobToMappers.get(jobid).keySet()){
					if (t.jobid.equals(jobid)){
						taskTrackers.get(this.jobToMappers.get(jobid).get(t).serviceName).mapSlotsFilled = Math.max(0, taskTrackers.get(this.jobToMappers.get(jobid).get(t).serviceName).mapSlotsFilled-1);
						if (this.taskTrackerToTasks.containsKey(this.jobToMappers.get(jobid).get(t).serviceName)){
							this.taskTrackerToTasks.get(this.jobToMappers.get(jobid).get(t).serviceName).remove(t);
						}
					}
				}
				this.jobToMappers.remove(jobid);
			}
			if (this.jobToReducers.get(jobid) != null){
				for (Task t: this.jobToReducers.get(jobid).keySet()){
					if (t.jobid.equals(jobid)){
						taskTrackers.get(this.jobToReducers.get(jobid).get(t).serviceName).reduceSlotsFilled = Math.max(0, taskTrackers.get(this.jobToReducers.get(jobid).get(t).serviceName).reduceSlotsFilled-1);
						if (this.taskTrackerToTasks.containsKey(this.jobToReducers.get(jobid).get(t).serviceName)){
							this.taskTrackerToTasks.get(this.jobToReducers.get(jobid).get(t).serviceName).remove(t);
						}
					}
				}
				this.jobToReducers.remove(jobid);
			}
			this.queuedMapTasks.remove(jobid);
			this.queuedReduceTasks.remove(jobid);
			this.jobs.get(jobid).info.setStatus(JobInfo.KILLED);
			return "Job Kill Successful: " + jobid;
		}
		else {
			return  "Job Kill Fail: " + jobid + " Job had failed before kill could be sent!";
		}
		
		
	}



	public void commit(String jobid)  {
		ConcurrentHashMap<Task, TaskTrackerInfo> tToTinfo = this.jobToReducers.get(jobid);
		for (Task t: tToTinfo.keySet()){
			TaskTrackerInfo info = tToTinfo.get(t);
			Registry reg;
			try {
				reg = LocateRegistry.getRegistry(taskTrackers.get(info.serviceName).IP, Environment.MapReduceInfo.TASKTRACKER_PORT);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface)reg.lookup(info.serviceName);
				taskTracker.uplodaToHDFS(jobid);
			} catch (RemoteException | NotBoundException e) {
				
				e.printStackTrace();
			}
		}

	}
	
	
}
