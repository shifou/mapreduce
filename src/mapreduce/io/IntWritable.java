package mapreduce.io;



public class IntWritable extends Writable {
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
	public int getHashValue() {
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
	
	

}
