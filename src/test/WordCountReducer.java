package test;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.Reducer;
import mapreduce.io.Context;
import mapreduce.io.IntWritable;
import mapreduce.io.Text;

public class WordCountReducer implements Reducer{

	public void reduce(Text key, Iterator values, Context context) throws IOException {
			 int sum = 0;
			 while (values.hasNext()) {
		            sum += ((IntWritable) values.next()).get();
		        }
		        context.write(key, new Text(sum));
		}

	

}
