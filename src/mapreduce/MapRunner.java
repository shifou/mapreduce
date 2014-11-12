package mapreduce;

import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.Writable;

public class MapRunner<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	private Mapper<K1, V1, K2, V2> mapper;
	public static void main(String[] args) {

	MapRunner<Writable, Writable, Writable, Writable> rm = new MapRunner<Writable, Writable, Writable, Writable>();
	/*
	 Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass = 
				rm.loadClass();
		

		Context<Writable, Writable> output = new Context<Writable, Writable>(rm.task);
		
		rm.mapper = (Mapper<Writable, Writable, Writable, Writable>) mapperClass.getConstructors()[0].newInstance();
		TextInputFormat recordReader = new TextInputFormat();
		while (recordReader.hasNext()) {
			Record<Text, Text> nextLine = recordReader.nextKeyValue();
			rm.mapper.map(nextLine.getKey(), nextLine.getValue(), output);
		}
		
		*/
}
}
