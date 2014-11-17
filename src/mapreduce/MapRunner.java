package mapreduce;

import java.lang.reflect.Constructor;

import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.Writable;

public class MapRunner<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> implements Runnable{
	public Mapper<K1, V1, K2, V2> mapper;
	public String jobid;
	public String taskid;
	public InputSplit block;
	public int tryNum;
	public Configuration conf;
	public String blockpath;
	public MapRunner(String file,String jid, String tid, InputSplit split, Configuration cf, int trynum)
	{
		blockpath=file;
		jobid=jid;
		taskid=tid;
		tryNum=trynum;
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
