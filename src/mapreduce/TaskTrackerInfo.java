package mapreduce;

import java.io.Serializable;

public class TaskTrackerInfo implements Serializable {

	public String IP;
	public String serviceName;
	public int health;
	public int slaveNum;
	public int slotsFilled;
	
	public TaskTrackerInfo(String IP, String serviceName, int health, int slave){
		this.IP = IP;
		this.serviceName = serviceName;
		this.health = health;
		this.slaveNum = slave;
		this.slotsFilled = 0;
	}
	
}
