package mapreduce.io;

import java.io.Serializable;

public class Record implements Comparable<Record>, Serializable{


	private static final long serialVersionUID = 7753244687717705109L;

	public String key;
	
	public String value;
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
		return (key.toString() + value.toString()).hashCode();
	}
	public Record (String k, String v) {
		
		this.key = k;
		
		this.value = v;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public String getValue() {
		return this.value;
	}
	public String toString()
	{
		return key.toString()+"\t"+value.toString();
	}
	public int compareTo(Record o) {
		return this.key.toString().compareTo(o.key.toString());
	}
}
