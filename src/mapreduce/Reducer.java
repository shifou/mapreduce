package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.io.Context;
import mapreduce.io.IntWritable;
import mapreduce.io.Text;
import mapreduce.io.Writable;


public interface Reducer<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public void reduce(Text key, Iterator<IntWritable> values, Context<K2, V2> context) throws IOException;
}
