package mapreduce;
public class Text extends Writable {


	private static final long serialVersionUID = 982087407444447124L;
	private String value;
	
	public Text(String text) {
		this.value = text;	
	}
	
	public Text() {
		value="";
	}

	public String getText() {
		return this.value;
	}

	@Override
	public int getHashValue() {
		return this.value.hashCode();
	}
	
	public String toString() {
		return this.value;
	}

	public void set(String nextToken) {
		value=nextToken;
	}
	
}