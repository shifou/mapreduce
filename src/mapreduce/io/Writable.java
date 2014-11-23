package mapreduce.io;
import java.io.Serializable;

public abstract class Writable implements Serializable{

	private static final long serialVersionUID = -6209822370726378744L;
	public abstract int hashCode();
	public abstract String toString();
	

}
