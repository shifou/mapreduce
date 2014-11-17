package mapreduce.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.Vector;

import main.Environment;

public class Context<K extends Writable, V extends Writable> {
	public String taskTrackerServiceName;
	public String jobid;
	public String taskid;
	public boolean mapTask;
	public TreeMap<K,V> reduceAns;
	public Vector<Record<K, V>> mapAns;
	public String outPath;
	// HDFS
	// |---tasktrackerServiceName
	//     |----jobid
	//			|----mapper
	//				|----taskid(1-Block_size)	
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
		if(mapornot)
		{
			mapAns = new Vector<Record<K, V>>();
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
			reduceAns= new TreeMap<K,V>();
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
		if(this.mapTask)
		{
			Record a =new Record(key,val);
			this.mapAns.add(a);
		}
		else
			this.reduceAns.put(key, val);
	}
}
