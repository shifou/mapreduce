package mapreduce;

import java.io.Serializable;

public class JobInfo implements Serializable {

	public static final int RUNNING = 1;
	public static final int SUCCEEDED = 2;
	public static final int FAILED = 3;
	public static final int WAITING = 4;
	public static final int KILLED = 5;

	
	private static final long serialVersionUID = 5437349646548467135L;
	
	
	private String ID;
	private int status;
	
	private int num_reducers;
	private int num_mappers;
	private int compl_reducers;
	private int compl_mappers;
	
	
	public JobInfo(String ID){
		this.ID = ID;
		this.num_mappers = 0;
		this.num_reducers = 0;
		this.compl_mappers = 0;
		this.compl_reducers = 0;
		this.status = JobInfo.WAITING;
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
	
	public void setNumMappers(int m){
		this.num_mappers = m;
	}
	
	public void setNumReducers(int r){
		this.num_reducers = r;
	}
	
	public void incrementComplMappers(){
		this.compl_mappers += 1;
	}
	
	public void incrementComplReducers(){
		this.compl_reducers += 1;
	}
}
