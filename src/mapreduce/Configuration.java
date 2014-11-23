package mapreduce;

import java.io.Serializable;


public class Configuration implements Serializable {


	private static final long serialVersionUID = 7052047607831227454L;

	private String jobName;
	public String jarName;
	public String jarPath;
	private String inputPath;
	private String outputPath;
	
	private String mapperClass;
	private String reducerClass;
	

	private String inputFormatClass;
	public String  outputFormatClass;

	
	private int numMapTasks;
	private int numReduceTasks;
	
	

	
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
	public String getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(String string) {
		this.mapperClass = string;
	}
	public String getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(String string) {
		this.reducerClass = string;
	}
	
	public void setNumMapTasks(int num) {
		this.numMapTasks = num;
	}
	
	public int getNumMapTasks() {
		return this.numMapTasks;
	}
	
	

	public void setInputFormat(String string) {
		this.inputFormatClass=string;
		
	}
	public String getInputFormat()
	{
		return this.inputFormatClass;
	}
	public void setOutputFormat(String string) {
		// TODO Auto-generated method stub
		this.outputFormatClass = string;
	}
	public String getOutputFormat()
	{
		return this.outputFormatClass;
	}

	public String getJarName() {
		return jarName;
	}

	public void setJarName(String jarName) {
		this.jarName = jarName;
	}

	public void setJarByPath(String jpath, String string2) {
		
		jarPath=jpath;
		jarName=string2;
	}
}