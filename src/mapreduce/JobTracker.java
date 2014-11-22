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
	public int jobID;
	public static ConcurrentHashMap<String, TaskTrackerInfo> taskTrackers;
	public ConcurrentHashMap<String, String> jobid2JarName;
	private ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>> jobToMappers; // JobID  -> Map of Task -> TaskTrackerInfo
	private ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>> jobToReducers;
	private ConcurrentLinkedQueue<Job> queuedJobs;
	private ConcurrentLinkedQueue<Task> queuedTasks;
	private ConcurrentHashMap<String, HashSet<TaskInfo>> completedMaps;
	private ConcurrentHashMap<String, HashSet<String>> jobToTaskTrackers;
	private Job currentJob;

	public JobTracker() {
		this.jobID = 1;
		this.jobs = new ConcurrentHashMap<String, Job>();
		this.taskTrackerAssignID = 1;
		JobTracker.taskTrackers = new ConcurrentHashMap<String, TaskTrackerInfo>();
		this.jobid2JarName = new ConcurrentHashMap<String, String>();
		this.jobToMappers = new ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>>();
		this.jobToReducers = new ConcurrentHashMap<String, ConcurrentHashMap<Task, TaskTrackerInfo>>();
		this.queuedJobs = new ConcurrentLinkedQueue<Job>();
		this.queuedTasks = new ConcurrentLinkedQueue<Task>();
		this.currentJob = null;
		this.completedMaps = new ConcurrentHashMap<String, HashSet<TaskInfo>>();
		this.jobToTaskTrackers = new ConcurrentHashMap<String, HashSet<String>>();
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
					.getRegistry(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
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
			if (Environment
					.createDirectory(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME
							+ "/" + jobid) == false)
				return "can not create jobid folder for jar\n";
			FileOutputStream out = new FileOutputStream(
					Environment.Dfs.DIRECTORY + "/"
							+ Environment.MapReduceInfo.JOBTRACKER_SERVICENAME
							+ "/" + jobid + "/" + jarname, true);
			byte[] buff = new byte[ct];
			jobid2JarName.put(jobid, jarname);
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
	public String join(String IP) throws RemoteException {
		String serviceName = "t" + this.taskTrackerAssignID;
		TaskTrackerInfo taskInfo = new TaskTrackerInfo(IP, serviceName,
				Environment.TIME_LIMIT, this.taskTrackerAssignID);
		JobTracker.taskTrackers.put(serviceName, taskInfo);

		this.taskTrackerAssignID++;
		return serviceName;

	}

	@Override
	public synchronized JobInfo submitJob(Job job) throws RemoteException {
		String jobid = String.format("%d", new Date().getTime()) + "_"
				+ this.jobID;
		jobID++;
		JobInfo info = new JobInfo(jobid);
		job.info = info;
		jobs.put(jobid, job);
		if (this.currentJob == null) {
			jobStart(job);
			this.currentJob = job;
		} else {
			this.queuedJobs.offer(job);
		}
		return info;
	}

	private void jobStart(Job job) {
		try {
			Registry r = LocateRegistry
					.getRegistry(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNode = (NameNodeRemoteInterface) r
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			InputSplit[] splits = nameNode.getSplit(job.getInputPath());
			job.info.setNumMappers(splits.length);
			for (int i = 0; i < splits.length; i++) {
				Task task = new Task(job.getJarClass(), Task.TaskType.Mapper,
						job.conf, null);
				task.setSplit(splits[i]);
				task.jobid = job.info.getID();
				task.taskid = "" + splits[i].getBlock().getID();
				allocateMapTask(task);
			}

		} catch (NotBoundException | RemoteException e) {

			e.printStackTrace();
		}
	}

	private void allocateMapTask(Task t) {
		HashSet<Integer> locations = t.getSplit().getLocations();
		String bestNode = null;
		int bestLoad = Environment.MapReduceInfo.SLOTS;
		t.locality = true;
		for (int i : locations) {
			String taskTrackerName = "t" + i;
			if ((JobTracker.taskTrackers.get(taskTrackerName).slotsFilled < Environment.MapReduceInfo.SLOTS)
					&& (JobTracker.taskTrackers.get(taskTrackerName).slotsFilled < bestLoad)) {
				bestNode = taskTrackerName;
				bestLoad = JobTracker.taskTrackers.get(taskTrackerName).slotsFilled;
			}
		}
		if (bestNode == null) {
			for (String s : JobTracker.taskTrackers.keySet()) {
				TaskTrackerInfo i = JobTracker.taskTrackers.get(s);
				if ((i.slotsFilled < bestLoad)
						&& (!locations.contains(i.slaveNum))) {
					bestLoad = i.slotsFilled;
					bestNode = s;
					t.locality = false;
				}
			}
		}
		if (this.jobToMappers.get(t.jobid) != null) {
			JobTracker.taskTrackers.get(bestNode).slotsFilled += 1;
			this.jobToMappers.get(t.jobid).put(t,
					JobTracker.taskTrackers.get(bestNode));
		} else {
			ConcurrentHashMap<Task, TaskTrackerInfo> temp = new ConcurrentHashMap<Task, TaskTrackerInfo>();
			JobTracker.taskTrackers.get(bestNode).slotsFilled += 1;
			temp.put(t, JobTracker.taskTrackers.get(bestNode));
			this.jobToMappers.put(t.jobid, temp);
		}
		if (bestNode == null) {
			this.queuedTasks.add(t);
		} else {
			Registry r;
			try {
				r = LocateRegistry.getRegistry(
						JobTracker.taskTrackers.get(bestNode).IP,
						Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface) r
						.lookup(bestNode);
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
	public Byte[] getJar(String jobid, long pos) throws RemoteException {
		if (jobid2JarName.containsKey(jobid) == false)
			return null;
		String name = this.jobid2JarName.get(jobid);
		try {
			RandomAccessFile raf = new RandomAccessFile(
					Environment.MapReduceInfo.JOBTRACKER_SERVICENAME + "/"
							+ jobid + "/" + name, "r");
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
			return null;
		}

	}

	@Override
	public void getReport(TaskInfo info) {
		TaskStatus status = info.st;
		Task.TaskType type = info.type;
		if (status == TaskStatus.FINISHED) {
			if (type == TaskType.Mapper) {
				for (Task t : this.jobToMappers.get(info.jobid).keySet()) {
					if (t.taskid.equals(info.taskid)) {
						String tracker = this.jobToMappers.get(info.jobid).get(
								t).serviceName;
						JobTracker.taskTrackers.get(tracker).slotsFilled = Math
								.max(0,
										JobTracker.taskTrackers.get(tracker).slotsFilled - 1);
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
						this.completedMaps.put(info.jobid, completed);
						if (this.jobs.get(info.jobid).info
								.getPrecentMapCompleted() == 100) {
							startReduceForJob(this.jobs.get(info.jobid));
						} else {
							if (!this.queuedTasks.isEmpty()) {
								Task task = this.queuedTasks.peek();
								if (task.jobid.equals(info.jobid)
										&& task.getType() == TaskType.Mapper) {
									allocateMapTask(this.queuedTasks.poll());
								}
							}
						}
					}
				}
			} else if (type == TaskType.Reducer) {
				for (Task t : this.jobToReducers.get(info.jobid).keySet()) {
					if (t.taskid.equals(info.taskid)) {
						String tracker = this.jobToReducers.get(info.jobid)
								.get(t).serviceName;
						JobTracker.taskTrackers.get(tracker).slotsFilled = Math
								.max(0,
										JobTracker.taskTrackers.get(tracker).slotsFilled - 1);
						this.jobToReducers.get(info.jobid).remove(t);
						this.jobs.get(info.jobid).info.incrementComplReducers();
						if (this.jobs.get(info.jobid).info
								.getPrecentReduceCompleted() == 100) {
							// Job has been completed! What should I do here?
						} 
					}
				}
			}
		} else if (status == TaskStatus.FAILED) {

		}

	}

	public static String findIp(String name) {
		if (taskTrackers.containsKey(name)) {
			return taskTrackers.get(name).IP;
		} else
			return "";
	}

	private void startReduceForJob(Job job) {
		HashSet<String> trackers = this.jobToTaskTrackers.get(job.info.getID());
		int i = 1;
		for (String tracker : trackers) {
			Task t = createReduceTask(i, job);
			i += 1;
			allocateReduceTask(job.info.getID(), t, tracker);
		}
	}

	private void allocateReduceTask(String jobID, Task t, String taskTrackerName) {
		if (JobTracker.taskTrackers.get(taskTrackerName).slotsFilled < Environment.MapReduceInfo.SLOTS){
			if (this.jobToReducers.get(jobID) != null) {
				JobTracker.taskTrackers.get(taskTrackerName).slotsFilled += 1;
				this.jobToReducers.get(jobID).put(t,
						JobTracker.taskTrackers.get(taskTrackerName));
			} else {
				ConcurrentHashMap<Task, TaskTrackerInfo> temp = new ConcurrentHashMap<Task, TaskTrackerInfo>();
				JobTracker.taskTrackers.get(taskTrackerName).slotsFilled += 1;
				temp.put(t, JobTracker.taskTrackers.get(taskTrackerName));
				this.jobToMappers.put(jobID, temp);
			}
			Registry r;
			try {
				r = LocateRegistry.getRegistry(
						JobTracker.taskTrackers.get(taskTrackerName).IP,
						Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface) r
						.lookup(taskTrackerName);
				taskTracker.runTask(t);
			} catch (RemoteException | NotBoundException e) {
				e.printStackTrace();
			}
			
			
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
		Task t = new Task(job.getJarClass(), TaskType.Reducer, job.conf, reduceMap);
		return t;
		
	}
}
