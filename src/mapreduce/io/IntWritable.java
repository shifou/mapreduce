package mapreduce.io;



public class IntWritable extends Writable implements Comparable<IntWritable> {
	private static final long serialVersionUID = -9120629456558794008L;
	private Integer value;
	
	public IntWritable(int val) {
		this.value = new Integer(val);
	}
	
	public IntWritable(String val) {
		this.value = new Integer(val); 
	}
	
	public int getValue() {
		return this.value.intValue();
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}
	
	@Override
	public String toString() {
		return "" + this.value.intValue();
	}

	public int get() {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public int compareTo(IntWritable o) {
		return this.value-o.value;
	}
	
	

}
