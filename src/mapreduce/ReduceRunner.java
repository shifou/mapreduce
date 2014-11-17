package mapreduce;

import java.lang.reflect.Constructor;

public class ReduceRunner implements Runnable{
	public Reducer<?, ?, ?, ?> reducer;
	public String jobid;
	public String taskid;
	public InputSplit block;
	public Configuration conf;
	public String blockpath;
	public ReduceRunner(String file,String jid, String tid, InputSplit split, Configuration cf)
	{
		blockpath=file;
		jobid=jid;
		taskid=tid;
		block=split;
		conf =cf;
	}
	@Override
	public void run() {
		
		Class<Reducer> mapClass;
		
			//step1 : get the programmer's Mapper class and Instantiate it
			mapClass = (Class<Reducer>) Class.forName(conf.getReducerClass());
			Constructor<Reducer> constructors = mapClass.getConstructor();
			reducer = constructors.newInstance();
			
			
		
		
		
	}	
	
}
