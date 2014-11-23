package test;

import java.util.ArrayList;
import mapreduce.Reducer;
import mapreduce.io.Context;

public class WordCountReducer implements Reducer{

	public void reduce(String key, ArrayList<String> value,
			Context context) {
		context.write(key, String.valueOf(value.size()));
	}
}
