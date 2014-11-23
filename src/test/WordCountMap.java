package test;

import java.io.IOException;
import java.util.StringTokenizer;

import mapreduce.Mapper;
import mapreduce.io.Context;
import mapreduce.io.IntWritable;

public class WordCountMap implements Mapper{
	 private final static IntWritable one = new IntWritable(1);
	       
	    public  void map(String key, String value, Context context) throws IOException {
	        //System.out.println
	        StringTokenizer tokenizer = new StringTokenizer(value);
	        System.out.println(value);
	        while (tokenizer.hasMoreTokens()) {
	            context.write(tokenizer.nextToken(), one.toString());
	           // System.out.println(word.toString()+"\t1");
	        }
	    }



}
