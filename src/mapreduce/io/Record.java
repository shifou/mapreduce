package mapreduce.io;

import java.io.Serializable;

public class Record implements Comparable<Record>, Serializable{

	private static final long serialVersionUID = -6321290028562240898L;

	public Object key;
	
	public Object value;
	public Record()
	{
		key=null;
		value=null;
	}
	@Override
	public boolean equals(Object object) {
		Record hold = (Record)object;
		return key.equals(hold.key) && value.equals(hold.value);
	}
	
	public int hashCode () {
		return key.toString().hashCode();
	}
	public Record (Object k, Object v) {
		
		this.key = k;
		
		this.value = v;
	}
	
	public Object getKey() {
		return this.key;
	}
	
	public Object getValue() {
		return this.value;
	}
	public String toString()
	{
		return key.toString()+"\t"+value.toString()+"\n";
	}
	public int compareTo(Record  o) {
		return this.key.hashCode()-o.key.hashCode();
	}
}
