package test;

import java.util.ArrayList;

import mapreduce.io.Context;

public class BigramReduce {
	public void reduce(String key, ArrayList value,
			Context context) {
		context.write(key, String.valueOf(value.size()));
	}

}
