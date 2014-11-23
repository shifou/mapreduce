package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.Reducer;
import mapreduce.io.Context;
import mapreduce.io.IntWritable;
import mapreduce.io.Text;

public class WordCountReducer implements Reducer{

	public void reduce(String key, ArrayList value,
			Context context) {
		context.write(key, String.valueOf(value.size()));
	}




	

}
