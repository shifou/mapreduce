package mapreduce;


import java.io.Serializable;

public class Task implements Serializable{
	

	private static final long serialVersionUID = 5694404790544124950L;

	public enum TaskType {
		Mapper, Reducer;
	}
	
	private InputSplit split;
	private Class<?> className;
	private TaskType type;
	
	public Task(Class<?> c, TaskType type){
		this.setJarClass(c);
		this.type = type;
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

public class Task {
	
>>>>>>> 8e04c501c6425933b4e00cf7dac7d2e31e64e94e
}
