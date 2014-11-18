package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import mapreduce.io.Context;
import mapreduce.io.LongWritable;
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
	public String taskServiceName;
	public MapRunner(String file,String jid, String tid, InputSplit split, Configuration cf,String tName)
	{
		blockpath=file;
		jobid=jid;
		taskid=tid;
		block=split;
		conf =cf;
		taskServiceName = tName;
	}
	@Override
	public void run() {
		
		Class<Mapper> mapClass;
		
			
			try {
				mapClass = (Class<Mapper>) Class.forName(conf.getMapperClass().getName());
				Constructor<Mapper> constructors = mapClass.getConstructor();
				mapper = constructors.newInstance();
				BufferedReader reader=new BufferedReader(new FileReader(new File(blockpath)));
				TextInputFormat read= new TextInputFormat();
				
				
				Context<?,?> ct;
				if(conf.getInputFormat().equals(TextInputFormat.class))
				{
					 ct = new Context<LongWritable,Text>(jobid, taskid, taskServiceName,true);
				}
			}catch (IOException e) {
					// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
			
		
		
		
	}	
	
}
