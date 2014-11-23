package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import main.Environment;
import mapreduce.Task.TaskType;
import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.Records;
import mapreduce.io.TextOutputFormat;

public class ReduceRunner implements Runnable {
	public Reducer  reducer;
	public String jobid;
	public String taskid;
	public Configuration conf;
	public String taskServiceName;
	public String jarpath;
	// HDFS
	// |---tasktrackerServiceName
	//     |----jobid
	//			|----mapper
	//				|----taskid(1-Block_size)_partition_id(1-tasktracker size)
	//			|----reducer
	//				|----taskid(1-Slave_size)
	//			xxx.jar

	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> loc;

	public ReduceRunner(String jid, String tid,
			ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> lc,
			Configuration cf, String tName, String path) {
		jobid = jid;
		taskid = tid;
		conf = cf;
		taskServiceName = tName;
		loc = lc;
		jarpath = path;
	}

	@Override
	public void run() {

		Class<Reducer> reduceClass;
		String outpath = "";
		try {
			reduceClass = load(this.jarpath);
			if(reduceClass==null)
			{

				TaskInfo res = new TaskInfo(TaskStatus.FAILED,
						"reducer load from jar failed" , jobid,
						this.taskServiceName, this.taskid,
						TaskType.Reducer, outpath);
				report(res);
				return;
			}
			Constructor<Reducer> constructors = reduceClass
					.getConstructor();
			reducer = constructors.newInstance();
			
			HashMap<String, List<String>> merge = new HashMap<String, List<String>>();
			for (String taskSerName : loc.keySet()) {
				if (taskSerName.equals(this.taskServiceName) == false) {
					Registry registry = LocateRegistry.getRegistry(
							Environment.Dfs.NAME_NODE_IP,
							Environment.MapReduceInfo.JOBTRACKER_PORT);
					JobTrackerRemoteInterface jobstub = (JobTrackerRemoteInterface) registry
							.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
						String ip = jobstub.findIp(taskSerName);
						if(ip.equals(""))
						{
							TaskInfo res = new TaskInfo(TaskStatus.FAILED,
									"retrieve partition " + taskid + " when locate map ip failed", jobid,
									this.taskServiceName, this.taskid,
									TaskType.Reducer, outpath);
							report(res);
							return;
						}
					 registry = LocateRegistry.getRegistry(
							ip,
							Environment.MapReduceInfo.TASKTRACKER_PORT);
					TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface) registry
							.lookup(taskSerName);
					for (Integer maptaskid : loc.get(taskSerName).keySet()) {
						Vector<Record> target = taskTracker.getPartition(
								this.jobid, maptaskid, this.taskid);
						if (target == null) {
							TaskInfo res = new TaskInfo(TaskStatus.FAILED,
									"retrieve partition " + taskid + " for map"
											+ maptaskid + " failed", jobid,
									this.taskServiceName, this.taskid,
									TaskType.Reducer, outpath);
							report(res);
							return;
						}
						for (Record a : target) {
							if (merge.containsKey(a.key))
								merge.get(a.key).add((String) a.value);
							else {
								List<String> hold = new ArrayList<String>();
								hold.add((String) a.value);
								merge.put((String) a.key, hold);
							}
						}
					}
				} else {
					String input = "", line;
					String path=Environment.Dfs.DIRECTORY+"/"+this.taskServiceName+"/"+this.jobid+"/mapper/";
					for (Integer maptaskid : loc.get(taskSerName).keySet()) 
					{
					File file = new File(path+"/"+maptaskid+"_"+this.taskid);
					if (file.exists() == false)
					{
						TaskInfo res = new TaskInfo(TaskStatus.FAILED,
								"retrieve partition from local" + taskid + " for map"
										+ maptaskid + " failed", jobid,
								this.taskServiceName, this.taskid,
								TaskType.Reducer, outpath);
						report(res);
						return;
					}
					try {
						BufferedReader reader = new BufferedReader(new FileReader(file));
						Vector<Record> target= new Vector<Record>();
							while ((line = reader.readLine()) != null) {
								String []tt=line.split("\t");
								Record inp =new Record(tt[0],tt[1]);
								target.add(inp);
							}
							for (Record a : target) {
								if (merge.containsKey(a.key))
									merge.get(a.key).add((String) a.value);
								else {
									List<String> hold = new ArrayList<String>();
									hold.add((String) a.value);
									merge.put((String) a.key, hold);
								}
							}
						} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						TaskInfo res = new TaskInfo(TaskStatus.FAILED,
								"read error when retrieve partition from local" + taskid + " for map"
										+ maptaskid + " failed", jobid,
								this.taskServiceName, this.taskid,
								TaskType.Reducer, outpath);
						report(res);
						return;
					}
					
				}
			}
			}
			Context ct = new Context(
					jobid, taskid, taskServiceName, false);

			for (String hold : merge.keySet()) {
				Records a = new Records(
						hold, merge.get(hold));
				reducer.reduce(a.getKey(), (ArrayList<String>)a.getValues(), ct);
			}
			outpath = Environment.Dfs.DIRECTORY + "/" + taskServiceName + "/"
					+ jobid + "/reducer/" ;
			if(TextOutputFormat.writeTolocal(outpath+ this.taskid, ct)==false)
			{
				TaskInfo res = new TaskInfo(TaskStatus.FAILED,
						"write reduce results to local failed for reduce" + taskid , jobid,
						this.taskServiceName, this.taskid,
						TaskType.Reducer, outpath);
				report(res);
			}
			TaskInfo res = new TaskInfo(TaskStatus.FINISHED, "finish reduce"
					+ taskid, jobid, this.taskServiceName, taskid,
					TaskType.Reducer, outpath);
			TaskTracker.updateFile(jobid,conf.getOutputPath());
			report(res);
			} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			TaskInfo res = new TaskInfo(TaskStatus.FAILED, "start reduce error"
					+ taskid, jobid, this.taskServiceName, taskid,
					TaskType.Reducer, outpath);
			report(res);
		}

	}

	public Class<Reducer> load(
			String jarFilePath) throws IOException, ClassNotFoundException {

		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();

		URL[] urls = { new URL("jar:file:" + jarFilePath + "!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);

		Class<Reducer> reducerClass = null;

		while (e.hasMoreElements()) {

			JarEntry je = e.nextElement();

			if (je.isDirectory() || !je.getName().endsWith(".class")) {
				continue;
			}

			String className = je.getName().substring(0,
					je.getName().length() - 6);
			className = className.replace('/', '.');
			if (className.equals(conf.getReducerClass())) {
				reducerClass = (Class<Reducer>) cl
						.loadClass(className);
			}
		}

		return reducerClass;

	}

	public void report(TaskInfo feedback) {
		try {
			TaskTracker.report(feedback);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
