package mapreduce.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
	//			xxx.jar
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
		}
		else
		{
			f=new File(outPath+"/reducer");
			if(f.exists()==false)
				f.mkdir();
			outPath= outPath+"/reducer";
		}
	}
	public void write(K key, V val) {
			ans.put(key, val);
	}
	public ConcurrentHashMap<Integer, String> writeToDisk(int partitionNum) {
		ConcurrentHashMap<Integer,String> lc= new ConcurrentHashMap<Integer,String>();
		String []data= new String[partitionNum];
		for(int i=0;i<partitionNum;i++)
			data[i]="";
		for(Writable k : this.ans.keySet())
		{
			int id = Math.abs(k.getHashValue())%partitionNum;
			data[id]+=(k+"\t"+ans.get(k)+"\n");
		}
		for(int i=0;i<partitionNum;i++)
		{
			try {
				String temp=outPath+"/"+taskid+"_"+i;
				FileOutputStream out = new FileOutputStream(temp);
				out.write(data[i].getBytes("UTF-8"));
				out.close();
				lc.put(i,temp);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return lc;
			}
		}
		return lc;
	}
}
