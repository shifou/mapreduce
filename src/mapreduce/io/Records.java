package mapreduce.io;

import java.lang.reflect.Constructor;
import java.util.List;

import mapreduce.Configuration;

public class Records {
	private String key;
	private List<String> valueList;
	
	public Records(String hold, List<String> valueList) {
		this.key = hold;
		this.valueList = valueList;
	}

	public String getKey() {
		return this.key;
	}
	
	public List<String> getValues() {
		return this.valueList;
	}
	
}
