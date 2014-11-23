package mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.rmi.RemoteException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import main.Environment;
import mapreduce.io.Context;

import java.net.URL;
import java.net.URLClassLoader;

import mapreduce.io.Record;
import mapreduce.io.RecordReader;

public class MapRunner implements Runnable {
	public Mapper mapper;
	public String jobid;
	public String taskid;
	public InputSplit block;
	public Configuration conf;
	public String taskServiceName;
	public int partitionNum;
	public String jarpath;
	public boolean local;

	public MapRunner(String jid, String tid, InputSplit split,
			Configuration cf, String tName, int num,String jarpath,boolean loc) {
		jobid = jid;
		taskid = tid;
		block = split;
		conf = cf;
		taskServiceName = tName;
		this.partitionNum = num;
		this.jarpath=jarpath;
		local=loc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		TaskInfo res;
		Class<Mapper> mapClass;
		System.out.println("begin running map thread: "+this.jobid+"\t"+this.taskid+"\t"+this.partitionNum+"\t"+this.taskServiceName+"\t"+this.jarpath);
		try {
			mapClass = load(jarpath);
			Constructor<Mapper> constructors = mapClass
					.getConstructor();
			mapper = constructors.newInstance();
			if(mapper==null)
			{
				res = new TaskInfo(TaskStatus.FAILED,
						"load map class from jar failed", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, null);
				report(res);
				return;
			}
			byte[] data = new byte[Environment.Dfs.BUF_SIZE];
			byte[] input ;
			if(local)
			{
				String folder= taskServiceName.substring(1);
				String path;
				if(block.block.getFolderName()==null)
					path = Environment.Dfs.DIRECTORY+"/d"+folder+"/"+block.block.getFileName()+"."+taskid;
				else
					path = Environment.Dfs.DIRECTORY+"/d"+folder+"/"+block.block.getFolderName()+"/"+block.block.getFileName()+"."+taskid;
				System.out.println("-----"+path);
				File jFile = new File(path);
				FileInputStream in = new FileInputStream(jFile);
				int len = in.read(data);
				if(len==-1)
				{
					res = new TaskInfo(TaskStatus.FAILED,
							"can not get the block data from local", this.jobid, this.taskid,this.taskServiceName,
							this.partitionNum, Task.TaskType.Mapper, null);
					report(res);
					return;
				}
				input =new byte[len];
			}
			else{
			int len = block.block.get(data);
			if (len == -1) {
				res = new TaskInfo(TaskStatus.FAILED,
						"can not get the block data", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, null);
				report(res);
				return;
			}
			input =new byte[len];
			}
			for(int i=0;i<input.length;i++)
				input[i]=data[i];
			System.out.println("finished get data------");
			Class<RecordReader> inputFormatClass = (Class<RecordReader>) Class
					.forName(conf.getInputFormat());
			Constructor<RecordReader> constuctor = inputFormatClass
					.getConstructor(String.class);
			RecordReader  read = constuctor.newInstance(input.toString());
			Context  ct = new Context (
					jobid, taskid, taskServiceName, true);
			while (read.hasNext()) {
				Record nextLine = read.nextKeyValue();
				mapper.map(nextLine.getKey(), nextLine.getValue(), ct);
			}

			System.out.println("finished map------");
			ConcurrentHashMap<Integer, String> loc = ct
					.writeToDisk(this.partitionNum);
			if (loc.size() != this.partitionNum) {
				res = new TaskInfo(TaskStatus.FAILED,
						"can not write the intermerdiate data to disk", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, null);
				report(res);
				return;
			} else
				System.out.println("finished------");
				res = new TaskInfo(TaskStatus.FINISHED,
						"ok", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, loc);
				report(res);
				return;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("++++++++++");
			res = new TaskInfo(TaskStatus.FAILED,
					e.getMessage(), this.jobid, this.taskid,this.taskServiceName,
					this.partitionNum, Task.TaskType.Mapper, null);
			report(res);
		
		} 
	}
	public Class<Mapper> load (String jarFilePath)
			throws IOException, ClassNotFoundException {
		
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<Mapper> mapperClass = null;
		
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(this.conf.getMapperClass())) {
            	mapperClass = (Class<Mapper>) cl.loadClass(className);
            }
        }
		
		return mapperClass;
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
