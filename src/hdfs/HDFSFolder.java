package hdfs;

import java.util.concurrent.ConcurrentHashMap;

public class HDFSFolder{
	public int fileSize;
	public String foldname;
	ConcurrentHashMap<String, HDFSFile> files;
	public int filesize() {
		// TODO Auto-generated method stub
		return fileSize;
	}
	public String delete() {
		for(String name:files.keySet())
		{
			HDFSFile hold = files.get(name);
			System.out.println(hold.delete());
		}
		return foldname;
	}
}
