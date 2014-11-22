package mapreduce;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
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

import main.Command;
import main.Environment;
import mapreduce.Task.TaskType;
import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.Records;
import mapreduce.io.Text;
import mapreduce.io.TextOutputFormat;
import mapreduce.io.Writable;

public class ReduceRunner implements Runnable{
	public Reducer<?, ?, ?, ?> reducer;
	public String jobid;
	public String taskid;
	public Configuration conf;
	public String taskServiceName;
	public String jarpath;
	// taskService name -> <maptid, pathforpattitionId>
	// HDFS
		// |---tasktrackerServiceName
		//     |----jobid
		//			|----mapper
		//				|----taskid(1-Block_size)_partition_id(1-tasktracker size)
		//			|----reducer
		//				|----taskid(1-Slave_size)
		
	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > loc;
	public ReduceRunner(String jid, String tid,ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > lc, Configuration cf,String tName,String path)
	{
		jobid=jid;
		taskid=tid;
		conf =cf;
		taskServiceName=tName;
		loc=lc;
		jarpath=path;
	}
	@Override
	public void run() {
		
		Class<Reducer<Writable,Writable,Writable,Writable>> reduceClass;
		String outpath="";
		try {
			reduceClass = load(this.jarpath);
			Constructor<Reducer<Writable,Writable,Writable,Writable>> constructors = reduceClass.getConstructor();
			reducer = constructors.newInstance();
			HashMap<Writable, List<Writable>> merge =new HashMap<Writable,List<Writable>>();
			for(String taskSerName : loc.keySet())
			{
				Registry registry = LocateRegistry.getRegistry(JobTracker.findIp(taskSerName), Environment.MapReduceInfo.TASKTRACKER_PORT);
				TaskTrackerRemoteInterface taskTracker = (TaskTrackerRemoteInterface) registry.lookup(taskSerName);
				for(Integer maptaskid : loc.get(taskSerName).keySet())
				{
					Vector<Record<?,?>> target =taskTracker.getPartition(this.jobid,maptaskid,this.taskid);
					if(target==null)
					{
						TaskInfo res= new TaskInfo(TaskStatus.FAILED,"retrieve partition "+taskid+" for "+maptaskid+" failed",jobid,this.taskServiceName, this.taskid, TaskType.Reducer, outpath);
						report(res);
					}
					for(Record<?,?> a: target)
					{
						if(merge.containsKey(a.key))
							merge.get(a.key).add(a.value);
						else
						{
							List<Writable> hold= new ArrayList<Writable>();
							hold.add(a.value);
							merge.put(a.key, hold);
						}
					}
				}
			}
			Context<Writable, Writable> ct = new Context<Writable, Writable>(
					jobid, taskid, taskServiceName, false);
			
			for(Writable hold : merge.keySet()) 
			{
				Records<Writable,Writable> a = new Records<Writable,Writable>(hold,merge.get(hold));
				reducer.reduce(a.getKey(), a.getValues().iterator(), ct);
			}
			outpath=Environment.Dfs.DIRECTORY+"/"+taskServiceName+"/"+jobid+"/reducer"+this.taskid;
			TextOutputFormat.writeTolocal(outpath, ct);
			Command a=new Command();
			a.putHandler(outpath,conf.getOutputPath()+"/part-"+this.taskid);
			TaskInfo res= new TaskInfo(TaskStatus.FINISHED, "finish reduce"+taskid, jobid,this.taskServiceName, taskid, TaskType.Reducer, outpath);
			report(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
			
	}	
	public Class<Reducer<Writable, Writable, Writable, Writable>> load (String jarFilePath)
			throws IOException, ClassNotFoundException {
		
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass = null;
		
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(conf.getReducerClass().getName())) {
            	reducerClass = (Class<Reducer<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
            }
        }
		
		return reducerClass;
		
	}
	
	public void report(TaskInfo feedback) {
		TaskTracker.report(feedback);
	}

	
}
