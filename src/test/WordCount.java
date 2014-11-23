package test;

import mapreduce.Configuration;
import mapreduce.Job;
import mapreduce.io.TextInputFormat;
import mapreduce.io.TextOutputFormat;
public class WordCount {
  

	 public static void main(String[] args)  {
	    Configuration conf = new Configuration();
	        

	    conf.setJobName("wordcount");
	    conf.setMapperClass("test.WordCountMap");
	    conf.setReducerClass("test.WordCountReducer");

	        
	    conf.setInputFormat("mapreduce.io.TextInputFormat");
	    conf.setOutputFormat("mapreduce.io.TextOutputFormat");
	        
	    conf.setInputPath(args[0]);
	    conf.setOutputPath(args[1]);
	    
	    conf.setJarByPath(args[2], args[3]);
	    Job job = new Job(conf);
	    job.waitForCompletion(true);
	 }
	       
	}