package hdfs;

import java.util.concurrent.ConcurrentHashMap;

public class HDFSFolder{
	public int fileSize;
	public String foldername;
	ConcurrentHashMap<String, HDFSFile> files;
	public HDFSFolder(String localFolderName) {
		foldername=localFolderName;
	}
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
		return foldername;
	}
	public String createFrom(String localFolderName) {
		for(String name:files.keySet())
		{
			HDFSFile hold = files.get(name);
			System.out.println(hold.delete());
		}
		return foldername;
	}
}
