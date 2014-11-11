package mapreduce;

import java.io.Serializable;


public class Job implements Serializable{
	private static final long serialVersionUID = -2872789904155820699L;
	public Configuration conf;
	public Job(Configuration conf, String string) {
		// TODO Auto-generated constructor stub
	}

	public void setOutputKeyClass(Class<Text> class1) {
		// TODO Auto-generated method stub
		
	}

	public void setOutputValueClass(Class<IntWritable> class1) {
		// TODO Auto-generated method stub
		
	}
	
	public void setMapperClass(Class class1) {
		// TODO Auto-generated method stub
		
	}

	public void setReducerClass(Class class1) {
		// TODO Auto-generated method stub
		
	}

	public void waitForCompletion(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public void setInputPath(Job job, String string) {
		// TODO Auto-generated method stub
		
	}

	public void setOutputPath(Job job, String string) {
		// TODO Auto-generated method stub
		
	}

	public void setOutputFormatClass(Class  class1) {
		// TODO Auto-generated method stub
		
	}

	public void setInputFormatClass(Class class1) {
		// TODO Auto-generated method stub
		
	}


}
