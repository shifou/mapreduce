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
import mapreduce.io.Writable;
public class WordCount {
    
	 public static class Map implements Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	        
	    public void map(LongWritable key, Text value, Context<Text,IntWritable> context) throws IOException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            context.write(word, one);
	        }
	    }
	 } 
	        
	 public static class Reduce implements Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterator<IntWritable> values,
				Context<Text, Text> context) throws IOException {
			 int sum = 0;
			 while (values.hasNext()) {
		            sum += values.next().get();
		        }
		        context.write(key, new Text(sum));
		}



	

	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf);
	    
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    job.setInputPath(args[0]);
	    job.setOutputPath(args[1]);
	    
	    job.setJarByPath(args[2], args[3], WordCount.class);
	        
	    job.waitForCompletion(true);
	 }
	       
	}