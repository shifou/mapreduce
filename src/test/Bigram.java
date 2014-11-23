package test;

import mapreduce.Configuration;
import mapreduce.Job;

public class Bigram {

	 public static void main(String[] args)  {
	    Configuration conf = new Configuration();
	        

	    conf.setJobName("bigram");
	    conf.setMapperClass("test.BigramMap");
	    conf.setReducerClass("test.BigramReduce");

	        
	    conf.setInputFormat("mapreduce.io.TextInputFormat");
	    conf.setOutputFormat("mapreduce.io.TextOutputFormat");
	        
	    conf.setInputPath(args[0]);
	    conf.setOutputPath(args[1]);
	    
	    conf.setJarByPath(args[2], args[3]);
	    Job job = new Job(conf);
	    job.waitForCompletion(true);
	 }
}
