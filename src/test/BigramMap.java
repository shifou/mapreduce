package test;

import java.io.IOException;
import java.util.StringTokenizer;

import mapreduce.io.Context;

public class BigramMap {
	 public  void map(String key, String value, Context context) throws IOException {
	        //System.out.println
	        StringTokenizer tokenizer = new StringTokenizer(value);
	        System.out.println(value);
	        while (tokenizer.hasMoreTokens()) {
	        	String hold = tokenizer.nextToken();
	            context.write(hold, one.toString());
	           // System.out.println(word.toString()+"\t1");
	        }
	    }

}
