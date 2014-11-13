package mapreduce;

public class MapperTask {
	
	private InputSplit split;
	private Class className;
	
	public MapperTask(InputSplit s, Class c){
		this.split = s;
		this.className = c;
	}

}
