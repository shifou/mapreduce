package mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;
import mapreduce.io.Context;
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

	public MapRunner(String jid, String tid, InputSplit split,
			Configuration cf, String tName, int num) {
		jobid = jid;
		taskid = tid;
		block = split;
		conf = cf;
		taskServiceName = tName;
		this.partitionNum = num;
	}

	@Override
	public void run() {

		Class<Mapper<Writable, Writable, Writable, Writable>> mapClass;
		try {
			mapClass = (Class<Mapper<Writable, Writable, Writable, Writable>>) Class
					.forName(conf.getMapperClass().getName());
			Constructor<Mapper<Writable, Writable, Writable, Writable>> constructors = mapClass
					.getConstructor();
			mapper = constructors.newInstance();
			byte[] data = new byte[Environment.Dfs.BUF_SIZE];
			int len = block.block.get(data);
			if (len == -1) {
				TaskInfo res = new TaskInfo(TaskStatus.FAILED,
						"can not get the block data", this.jobid, this.taskid,
						this.partitionNum, Task.TaskType.Mapper, null);
				report(res);
				return;
			}
			Class<RecordReader> inputFormatClass = (Class<RecordReader>) Class
					.forName(conf.getInputFormat().getName());
			Constructor<RecordReader> constuctor = inputFormatClass
					.getConstructor(String.class);
			RecordReader read = constuctor.newInstance(data.toString());
			Context<Writable, Writable> ct = new Context<Writable, Writable>(
					jobid, taskid, taskServiceName, true);
			while (read.hasNext()) {
				Record<Writable, Writable> nextLine = read.nextKeyValue();
				mapper.map(nextLine.getKey(), nextLine.getValue(), ct);
			}

			ConcurrentHashMap<Integer, String> loc = ct
					.writeToDisk(this.partitionNum);
			if (loc.size() != this.partitionNum) {

				report(null);
			} else
				report(null);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
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

	public void report(TaskInfo feedback) {

	}

}
