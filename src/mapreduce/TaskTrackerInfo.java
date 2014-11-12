package mapreduce;

import java.io.Serializable;

public class TaskTrackerInfo implements Serializable {

	public String IP;
	public String serviceName;
	public int health;
	
	public TaskTrackerInfo(String IP, String serviceName, int health){
		this.IP = IP;
		this.serviceName = serviceName;
		this.health = health;
	}
	
}