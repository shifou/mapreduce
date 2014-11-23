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
	public ArrayList<Record> ans;
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
		ans= new ArrayList<Record>();
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
	public void write(Object key, Object val) {
			Record hold =new Record(key,val);
			ans.add(hold);
			System.out.println("????"+key.toString()+" "+val.toString());
			
		}
	public ConcurrentHashMap<Integer, String> writeToDisk(int partitionNum) {
		System.out.println(this.ans.size());
		ConcurrentHashMap<Integer,String> lc= new ConcurrentHashMap<Integer,String>();
		String []data= new String[partitionNum];
		for(int i=0;i<partitionNum;i++)
			data[i]="";
		for(int i=0;i<ans.size();i++)
		{
			Record k = ans.get(i);
			int id = Math.abs((k.getKey().toString().hashCode()) % partitionNum);
			data[id]+=(k.key.toString()+"\t"+k.value.toString()+"\n");
		}
		System.out.println(outPath);
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
