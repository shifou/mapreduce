package test;

import java.io.IOException;
import java.util.StringTokenizer;

import mapreduce.io.Context;

public class BigramMap {
	 public  void map(String key, String value, Context context) throws IOException {
		 	String line = value.trim().toLowerCase();
			line = line.replaceAll("[^a-z ]", " ");
			line = line.trim().replaceAll(" +", " ");
			String[] split = line.split(" ");
	        for(int i=0;i<split.length-1;i++)
	        {
	        		context.write(split[i]+" "+split[i+1], "1");
	        }
	       
	    }

}
