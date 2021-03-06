package mapreduce.io;

import java.io.IOException;
import java.io.Serializable;

public abstract class RecordReader implements Serializable {
	

	private static final long serialVersionUID = -7591521790480016188L;

	public abstract Record nextKeyValue();
	
	public abstract boolean hasNext();


}
