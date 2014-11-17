package mapreduce.io;

import java.io.Serializable;

public class Record<KEY extends Writable, VALUE extends Writable> implements Comparable<Record<KEY, VALUE>>, Serializable{

	private static final long serialVersionUID = -6321290028562240898L;

	KEY key;
	
	VALUE value;
	public Record()
	{
		key=null;
		value=null;
	}
	
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
	public String toString()
	{
		return key.toString()+"\t"+value.toString()+"\n";
	}
	public int compareTo(Record<KEY, VALUE> o) {
		return this.key.toString().compareTo(o.key.toString());
	}
}
