package test;

import java.io.IOException;
import java.util.StringTokenizer;

import mapreduce.Mapper;
import mapreduce.io.Context;

public class WordCountMap implements Mapper{

	    public  void map(String key, String value, Context context) throws IOException {
	        //System.out.println
	        StringTokenizer tokenizer = new StringTokenizer(value);
	        System.out.println(value);
	        while (tokenizer.hasMoreTokens()) {
	        	String hold = tokenizer.nextToken();
	            context.write(hold, "1");
	           // System.out.println(word.toString()+"\t1");
	        }
	    }



}
