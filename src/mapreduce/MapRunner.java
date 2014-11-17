package mapreduce;

import java.lang.reflect.Constructor;

import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.Writable;

public class MapRunner implements Runnable{
	public Mapper<?, ?, ?, ?> mapper;
	public String jobid;
	public String taskid;
	public InputSplit block;
	public Configuration conf;
	public String blockpath;
	public MapRunner(String file,String jid, String tid, InputSplit split, Configuration cf)
	{
		blockpath=file;
		jobid=jid;
		taskid=tid;
		block=split;
		conf =cf;
	}
	@Override
	public void run() {
		
		Class<Mapper> mapClass;
		
			//step1 : get the programmer's Mapper class and Instantiate it
			mapClass = (Class<Mapper>) Class.forName(conf.getMapperClass());
			Constructor<Mapper> constructors = mapClass.getConstructor();
			mapper = constructors.newInstance();
			
			
		
		
		
	}	
	
}
