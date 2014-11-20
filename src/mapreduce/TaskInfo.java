package mapreduce;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Task.TaskType;

public class TaskInfo implements Serializable {

	public TaskInfo(TaskStatus stt, String string, String jobid2,
			String taskid2, int partitionNum, TaskType tp, ConcurrentHashMap<Integer, String> loc) {
		st=stt;
		reason = string;
		jobid=jobid2;
		taskid=taskid2;
		partionNum=partitionNum;
		type=tp;
		locations = loc;
	}
	private static final long serialVersionUID = 5025490142898429108L;
	public String jobid;
	public String taskid;
	public Task.TaskType type;
	public TaskStatus st;
	public int partionNum;
	public String reason;
	public ConcurrentHashMap<Integer, String> locations;
}
