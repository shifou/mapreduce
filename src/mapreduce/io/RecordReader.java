package mapreduce.io;

import java.io.IOException;
import java.io.Serializable;

public abstract class RecordReader<K extends Writable,V extends Writable> implements Serializable {
	

	private static final long serialVersionUID = -7591521790480016188L;

	public abstract Record<K, V> nextKeyValue();
	
	public abstract boolean hasNext();


}
