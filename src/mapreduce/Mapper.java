package mapreduce;

import java.io.IOException;

import mapreduce.io.Context;
import mapreduce.io.Writable;

public interface Mapper {
	
	public void map(String key, String value, Context context) throws IOException;
}