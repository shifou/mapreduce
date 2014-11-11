package mapreduce;

import java.io.IOException;
import java.util.Iterator;


public abstract class Reducer<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public abstract void reduce(Text key, Iterator<IntWritable> values, Context<K2, V2> context) throws IOException;
}
