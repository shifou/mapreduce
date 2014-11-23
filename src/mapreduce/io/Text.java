package mapreduce.io;


public class Text extends Writable implements Comparable<Text> {


	private static final long serialVersionUID = 982087407444447124L;
	private String value;
	
	public Text(String text) {
		this.value = text;	
	}
	
	public Text() {
		value="";
	}

	public Text(int sum) {
		value=String.valueOf(sum);
	}

	public String getText() {
		return this.value;
	}

	@Override
	public int hashCode() {
		return this.value.hashCode();
	}
	
	public String toString() {
		return this.value;
	}

	public void set(String nextToken) {
		value=nextToken;
	}

	@Override
	public int compareTo(Text o) {
		return this.value.compareTo(o.value);
	}
	
}