package test;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.Configuration;
import mapreduce.Job;
import mapreduce.Mapper;
import mapreduce.Reducer;
import mapreduce.io.Context;
import mapreduce.io.IntWritable;
import mapreduce.io.LongWritable;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.TextOutputFormat;
public class WordCount {
  

	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf);
	    
	    if (test.WordCountMap.class == null){
	    	System.out.println("WTF");
	    }
	    
	    job.setMapperClass(test.WordCountMap.class);
	    job.setReducerClass(test.WordCountReducer.class);

	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    job.setInputPath(args[0]);
	    job.setOutputPath(args[1]);
	    
	    job.setJarByPath(args[2], args[3], WordCount.class);
	        
	    job.waitForCompletion(true);
	 }
	       
	}