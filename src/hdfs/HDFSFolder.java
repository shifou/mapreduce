package hdfs;

import java.util.concurrent.ConcurrentHashMap;

public class HDFSFolder implements HDFSAbstraction{
	public int fileSize;
	public String foldname;
	ConcurrentHashMap<String, HDFSAbstraction> files;
	public int filesize() {
		// TODO Auto-generated method stub
		return fileSize;
	}
	public String delete() {
		for(int i=0;i<folder.filesize();i++)
		{
			HDFSFile hold = files.
			System.out.println(dfs_folder.delete(i));
		}
	}

}
