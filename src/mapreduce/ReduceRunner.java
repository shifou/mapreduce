package mapreduce;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.io.Writable;

public class ReduceRunner implements Runnable{
	public Reducer<?, ?, ?, ?> reducer;
	public String jobid;
	public String taskid;
	public Configuration conf;
	public String taskServiceName;
	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > loc;
	public ReduceRunner(String jid, String tid,ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > lc, Configuration cf,String tName)
	{
		jobid=jid;
		taskid=tid;
		conf =cf;
		taskServiceName=tName;
		loc=lc;
	}
	@Override
	public void run() {
		
		Class<Reducer<Writable,Writable,Writable,Writable>> reduceClass;
		
		try {
			reduceClass = (Class<Reducer<Writable,Writable,Writable,Writable>>) Class.forName(conf.getReducerClass().getName());
			Constructor<Reducer<Writable,Writable,Writable,Writable>> constructors = reduceClass.getConstructor();
			reducer = constructors.newInstance();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
			
	}	
	public void report(TaskInfo feedback) {
		
	}

	
}
