package mapreduce.io;

import java.lang.reflect.Constructor;
import java.util.List;

import mapreduce.Configuration;

public class Records<K1 extends Writable, V1 extends Writable> {
	private K1 key;
	private List<V1> valueList;
	
	public Records(K1 hold, List<V1> valueList) {
		this.key = hold;
		this.valueList = valueList;
	}

	public K1 getKey() {
		return this.key;
	}
	
	public List<V1> getValues() {
		return this.valueList;
	}
	
}
