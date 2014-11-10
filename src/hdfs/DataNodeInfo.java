package hdfs;

import java.io.Serializable;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class DataNodeInfo implements Comparable<DataNodeInfo>,Serializable {

	private static final long serialVersionUID = -6929193118742452681L;
	public String serviceName;
	public String ip;
	public int blockload;
	public int lostTime;
	public HashSet<String> files ;
	public ConcurrentHashMap<String,HashSet<String>> folders ;
	
	public DataNodeInfo(String ip2, String ans, int ll) {
		ip = ip2;
		serviceName = ans;
		files = new HashSet<String>();
		folders =new ConcurrentHashMap<String,HashSet<String>>();
		blockload = 0;
		lostTime=ll;
	}

	@Override
	public int compareTo(DataNodeInfo o) {
		int temp = this.blockload - o.blockload;
		if(temp==0)
			return this.serviceName.compareTo(o.serviceName);
		else
			return temp;
	}
}
