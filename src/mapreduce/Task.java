package mapreduce;


import java.io.Serializable;

public class Task implements Serializable{
	

	private static final long serialVersionUID = 5694404790544124950L;

	public enum TaskType {
		Mapper, Reducer;
	}
	
	private Configuration config;
	private InputSplit split;
	private Class<?> className;
	private TaskType type;
	
	public Task(Class<?> c, TaskType type, Configuration con){
		this.setJarClass(c);
		this.type = type;
		this.config = con;
	}

	public InputSplit getSplit() {
		return split;
	}

	public void setSplit(InputSplit split) {
		this.split = split;
	}

	public Class<?> getJarClass() {
		return className;
	}

	public void setJarClass(Class<?> className) {
		this.className = className;
	}
	
	public TaskType getType(){
		return this.type;
	}

}
