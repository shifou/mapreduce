package mapreduce;

import java.io.IOException;

import mapreduce.io.Context;

public interface Mapper {
	
	public void map(String key, String value, Context context) throws IOException;
}