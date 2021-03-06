package test;

import java.util.ArrayList;

import mapreduce.Reducer;
import mapreduce.io.Context;

public class BigramReduce implements Reducer {
	public void reduce(String key, ArrayList<String> value,
			Context context) {
		int sum = 0;
		for (String val : value) {
			sum += Integer.parseInt(val);
		}
		context.write(key, String.valueOf(sum));
	}

}
