package mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import main.Environment;
import mapreduce.io.Context;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import mapreduce.io.LongWritable;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.Writable;

public class MapRunner implements Runnable {
	public Mapper<Writable, Writable, Writable, Writable> mapper;
	public String jobid;
	public String taskid;
	public InputSplit block;
	public Configuration conf;
	public String taskServiceName;
	public int partitionNum;
	public String jarpath;

	public MapRunner(String jid, String tid, InputSplit split,
			Configuration cf, String tName, int num,String jarpath) {
		jobid = jid;
		taskid = tid;
		block = split;
		conf = cf;
		taskServiceName = tName;
		this.partitionNum = num;
		this.jarpath=jarpath;
	}

	@Override
	public void run() {
		TaskInfo res;
		Class<Mapper<Writable, Writable, Writable, Writable>> mapClass;
		try {
			mapClass = load(jarpath);
			Constructor<Mapper<Writable, Writable, Writable, Writable>> constructors = mapClass
					.getConstructor();
			mapper = constructors.newInstance();
			byte[] data = new byte[Environment.Dfs.BUF_SIZE];
			int len = block.block.get(data);
			if (len == -1) {
				res = new TaskInfo(TaskStatus.FAILED,
						"can not get the block data", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, null);
				report(res);
				return;
			}
			Class<RecordReader<Writable,Writable>> inputFormatClass = (Class<RecordReader<Writable,Writable>>) Class
					.forName(conf.getInputFormat().getName());
			Constructor<RecordReader<Writable,Writable>> constuctor = inputFormatClass
					.getConstructor(String.class);
			RecordReader<Writable,Writable> read = constuctor.newInstance(data.toString());
			Context<Writable, Writable> ct = new Context<Writable, Writable>(
					jobid, taskid, taskServiceName, true);
			while (read.hasNext()) {
				Record<Writable, Writable> nextLine = read.nextKeyValue();
				mapper.map(nextLine.getKey(), nextLine.getValue(), ct);
			}

			ConcurrentHashMap<Integer, String> loc = ct
					.writeToDisk(this.partitionNum);
			if (loc.size() != this.partitionNum) {
				res = new TaskInfo(TaskStatus.FAILED,
						"can not write the intermerdiate data to disk", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, null);
				report(res);
			} else
				res = new TaskInfo(TaskStatus.FINISHED,
						"can not write the intermerdiate data to disk", this.jobid, this.taskid,this.taskServiceName,
						this.partitionNum, Task.TaskType.Mapper, loc);
				report(res);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			res = new TaskInfo(TaskStatus.FAILED,
					e.getMessage(), this.jobid, this.taskid,this.taskServiceName,
					this.partitionNum, Task.TaskType.Mapper, null);
			report(res);
			e.printStackTrace();
		} 
	}
	public Class<Mapper<Writable, Writable, Writable, Writable>> load (String jarFilePath)
			throws IOException, ClassNotFoundException {
		
		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass = null;
		
		/* Iterate .class files */
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0, je.getName().length() - 6);
            className = className.replace('/', '.');
            if (className.equals(this.conf.getMapperClass().getName())) {
            	mapperClass = (Class<Mapper<Writable, Writable, Writable, Writable>>) cl.loadClass(className);
            }
        }
		
		return mapperClass;
	}

	public void report(TaskInfo feedback) {
		
	}

}
