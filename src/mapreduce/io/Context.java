package mapreduce.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class Context<K extends Writable, V extends Writable> {
	public String taskTrackerServiceName;
	public String jobid;
	public String taskid;
	public boolean mapTask;
	public TreeMap<K,V> ans;
	public String outPath;
	// HDFS
	// |---tasktrackerServiceName
	//     |----jobid
	//			|----mapper
	//				|----taskid(1-Block_size)_partition_id(1-tasktracker size)
	//			|----reducer
	//				|----taskid(1-Slave_size)
	public Context(String jid, String tid, String taskName,boolean mapornot) throws IOException
	{
		taskTrackerServiceName = taskName;
		jobid= jid;
		taskid=tid;
		mapTask=mapornot;
		File f= new File(Environment.Dfs.DIRECTORY+"/"+taskTrackerServiceName+"/"+jid);
		if(f.exists()==false)
		{
			f.mkdir();
		}
		outPath=Environment.Dfs.DIRECTORY+"/"+taskTrackerServiceName+"/"+jid;
		ans= new TreeMap<K,V>();
		if(mapornot)
		{
			f=new File(outPath+"/mapper");
			if(f.exists()==false)
				f.mkdir();
			outPath= outPath+"/mapper";
			f=new File(outPath+"/"+tid);
			if(f.exists()==false)
				f.createNewFile();
			outPath = outPath+"/"+tid;
		}
		else
		{
			f=new File(outPath+"/reducer");
			if(f.exists()==false)
				f.mkdir();
			outPath= outPath+"/reducer";
			f=new File(outPath+"/"+tid);
			if(f.exists()==false)
				f.createNewFile();
			outPath = outPath+"/"+tid;
		}
	}
	public void write(K key, V val) {
			ans.put(key, val);
	}
	public ConcurrentHashMap<Integer, String> writeToDisk(int partitionNum) {
		// TODO Auto-generated method stub
		return null;
	}
}
