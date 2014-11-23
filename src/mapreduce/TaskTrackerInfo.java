package mapreduce;

import java.io.Serializable;

public class TaskTrackerInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -11753950736554393L;
	public String IP;
	public String serviceName;
	public int health;
	public int slaveNum;
	public int mapSlotsFilled;
	public int reduceSlotsFilled;
	
	public TaskTrackerInfo(String IP, String serviceName, int health, int slave){
		this.IP = IP;
		this.serviceName = serviceName;
		this.health = health;
		this.slaveNum = slave;
		this.mapSlotsFilled = 0;
		this.reduceSlotsFilled = 0;
	}
	
}
