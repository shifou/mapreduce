package mapreduce;

import java.io.Serializable;

public class TaskInfo implements Serializable {

	private static final long serialVersionUID = 5025490142898429108L;
	public String jobid;
	public String taskid;
	public boolean maptask;
	public TaskStatus st;
	public String outputLocation;
	
}
