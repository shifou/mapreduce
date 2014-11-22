package mapreduce.io;

import java.util.List;

public class Records<K extends Writable, V extends Writable> {
	private K key;
	private List<V> valueList;
	
	public Records(K key, List<V> valueList) {
		this.key = key;
		this.valueList = valueList;
	}
	
	public K getKey() {
		return this.key;
	}
	
	public List<V> getValues() {
		return this.valueList;
	}
	
}
