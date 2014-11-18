package mapreduce;

import java.io.Serializable;

public class Configuration implements Serializable {


	private static final long serialVersionUID = 7052047607831227454L;

	private String jobName;
	
	private String inputPath;
	private String outputPath;
	
	private Class<?> mapperClass;
	private Class<?> reducerClass;
	

	private Class<?> mapOutputKeyClass;
	private Class<?> mapOutputValueClass;
	private Class<?> inputFormatClass;
	public Class<?>  inputKeyClass;
	public Class<?>  inputValClass;
	public Class<?>  outputFormatClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	
	private int numMapTasks;
	private int numReduceTasks;
	
	private int priorityLevel;
	
	public void setPriority(int level) {
		this.priorityLevel = level;
	}
	
	public int getPriority() {
		return this.priorityLevel;
	}
	
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	
	public String getJobName() {
		return this.jobName;
	}
	
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	
	public String getInputPath() {
		return this.inputPath;
	}
	
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	public String getOutputPath() {
		return this.outputPath;
	}
	public Class<?> getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(Class<?> mapperClass) {
		this.mapperClass = mapperClass;
	}
	public Class<?> getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(Class<?> reducerClass) {
		this.reducerClass = reducerClass;
	}
	public void setMapOutputKeyClass(Class<?> theClass) {
		this.mapOutputKeyClass = theClass;
	}
	
	
	public Class<?> getMapOutputKeyClass() {
		return this.mapOutputKeyClass;
	}
	
	public void setMapOutputValueClass(Class<?> theClass) {
		this.mapOutputValueClass = theClass;
	}
	
	public Class<?> getMapOutputValueClass() {
		return this.mapOutputValueClass;
	}
	
	public void setOutputKeyClass(Class<?> theClass) {
		this.outputKeyClass = theClass;
	}
	
	public Class<?> getOutputKeyClass() {
		return this.outputKeyClass;
	}
	
	public void setOutputValueClass(Class<?> theClass) {
		this.outputValueClass = theClass;
	}
	
	public Class<?> getOutputValueClass() {
		return this.outputValueClass;
	}
	
	public void setNumMapTasks(int num) {
		this.numMapTasks = num;
	}
	
	public int getNumMapTasks() {
		return this.numMapTasks;
	}
	
	public void setNumReduceTasks(int num) {
		this.numReduceTasks = num;
	}
	
	public int getNumReduceTasks() {
		return this.numReduceTasks;
	}

	public void setInputFormat(Class<?> class1) {
		this.inputFormatClass=class1;
		
	}
	public Class<?> getInputFormat()
	{
		return this.inputFormatClass;
	}
	public void setOutputFormat(Class<?> class1) {
		// TODO Auto-generated method stub
		this.outputFormatClass = class1;
	}
	public Class<?> getOutputFormat()
	{
		return this.outputFormatClass;
	}
}