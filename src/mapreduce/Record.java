package mapreduce;

import java.io.Serializable;

public class Record<KEY extends Writable, VALUE extends Writable> implements Comparable<Record<KEY, VALUE>>, Serializable{
	KEY key;
	
	VALUE value;

	
	public Record (KEY k, VALUE v) {
		
		this.key = k;
		
		this.value = v;
	}
	
	public KEY getKey() {
		return this.key;
	}
	
	public VALUE getValue() {
		return this.value;
	}
	
	public int compareTo(Record<KEY, VALUE> o) {
		return this.key.toString().compareTo(o.key.toString());
	}
}
