package test;

import java.util.ArrayList;

import mapreduce.io.Context;

public class BigramReduce {
	public void reduce(String key, ArrayList<String> value,
			Context context) {
		int sum = 0;
		for (String val : value) {
			sum += Integer.parseInt(val);
		}
		context.write(key, String.valueOf(sum));
	}

}
