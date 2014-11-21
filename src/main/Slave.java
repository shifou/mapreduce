package main;


import mapreduce.TaskTracker;
import hdfs.DataNode;

public class Slave {
	public static DataNode datanode;
	public static TaskTracker taskTracker;

	public static void main(String[] args) {
		try {
			if( Environment.configure()==false)
			{
				System.err.println("please configure hdfs and mapred first");
				System.exit(1);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("please configure hdfs and mapred first");
			
			System.exit(1);
		}
		datanode = new DataNode(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
		if (!datanode.start()){
			System.err.println("DataNode failed to start. Exiting..");
			System.exit(1);
		}
		
		taskTracker = new TaskTracker();
		if (!taskTracker.start()){
			System.err.println("TaskTracker failed to start. Exiting..");
		}
		
	}
}
