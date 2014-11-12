package mapreduce;

import java.io.Serializable;

public class JobInfo implements Serializable {

	public static final int RUNNING = 1;
	public static final int SUCCEEDED = 2;
	public static final int FAILED = 3;

	
	private static final long serialVersionUID = 5437349646548467135L;
	
	
	private String ID;
	private int status;
	
	private int num_reducers;
	private int num_mappers;
	private int compl_reducers;
	private int compl_mappers;
	
	
	public JobInfo(String ID){
		this.ID = ID;
	}
	
	public void setStatus(int status){
		this.status = status;
	}
	
	public int getStatus(){
		return this.status;
	}
	
	public String getID(){
		return this.ID;
	}
	
	public int getPrecentMapCompleted(){
		return (this.compl_mappers * 100)/this.num_mappers;
	}
	
	public int getPrecentReduceCompleted(){
		return (this.compl_reducers * 100)/this.num_reducers;
	}
}
