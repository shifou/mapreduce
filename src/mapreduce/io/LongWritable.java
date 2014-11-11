package mapreduce.io;


public class LongWritable extends Writable{
private static final long serialVersionUID = 494950949886660335L;
	private Long value;
	
	public LongWritable(long val) {
		this.value = new Long(val); //Initiate value
	}
	
	public LongWritable(String val) {
		this.value = new Long(val);
	}
	
	public long getValue() {
		return this.value.longValue();
	}

	@Override
	public int getHashValue() {
		return this.value.hashCode();
	}

	@Override
	public String toString() {
		return "" + this.value;
	}
	
	
}
