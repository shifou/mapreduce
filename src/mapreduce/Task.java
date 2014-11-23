package mapreduce;


import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class Task implements Serializable{
	

	private static final long serialVersionUID = 5694404790544124950L;

	public enum TaskType {
		Mapper, Reducer;
	}
	public ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > mploc;
	
	public  Configuration config;
	public boolean locality;

	private InputSplit split;
	private TaskType type;

	public String jobid;
	public String taskid;
	public int reduceNum;

	public Task(TaskType type, Configuration con,	ConcurrentHashMap<String, ConcurrentHashMap<Integer, String> > lc ){
		mploc=lc;
		this.type = type;
		this.config = con;
	}
	
	public InputSplit getSplit() {
		return split;
	}

	public void setSplit(InputSplit split) {
		this.split = split;
	}


	
	public TaskType getType(){
		return this.type;
	}
	
	public Configuration getConfig(){
		return this.config;
	}

}
