package mapreduce;

import java.io.IOException;
import java.io.Serializable;

public abstract class RecordReader<KEYIN extends Writable,VALUEIN extends Writable> implements Serializable {
	

	private static final long serialVersionUID = -7591521790480016188L;
	protected InputSplit split;
	
	public RecordReader(InputSplit split) {
		this.split = split;
	}
}
