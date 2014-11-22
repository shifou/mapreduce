package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.io.Context;
import mapreduce.io.IntWritable;
import mapreduce.io.Text;
import mapreduce.io.Writable;


public interface Reducer<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public void reduce(Writable writable, Iterator<Writable> iterator, Context<Writable, Writable> ct ) throws IOException;
}
