package hdfs;

import java.io.Serializable;

public class DataNodeInfo implements Comparable<DataNodeInfo>,Serializable {

	private static final long serialVersionUID = -6929193118742452681L;
	public String serviceName;
	public String ip;
	public int blockload;

	public DataNodeInfo(String ip2, String ans) {
		ip = ip2;
		serviceName = ans;
		blockload = 0;
	}

	@Override
	public int compareTo(DataNodeInfo o) {
		return this.blockload - o.blockload;
	}
}
