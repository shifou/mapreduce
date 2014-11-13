package mapreduce;

import hdfs.DataNodeInfo;
import hdfs.HDFSBlock;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class InputSplit implements Serializable {

	private static final long serialVersionUID = -3216548064249420892L;
	public HDFSBlock block;
	public int fileid;
	public InputSplit(int id, HDFSBlock hold)
	{
		fileid=id;
		block=hold;
	}
}
