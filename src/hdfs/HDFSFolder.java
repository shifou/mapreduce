package hdfs;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class HDFSFolder{
	public int fileSize;
	public String foldername;
	public ConcurrentHashMap<String, HDFSFile> files;
	public HDFSFolder(String localFolderName) {
		foldername=localFolderName;
		files =new ConcurrentHashMap<String, HDFSFile> ();
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
		File f=new File(localFolderName);
		File [] ff= f.listFiles();
		System.out.println("copy from folder "+ localFolderName);
		for(File each : ff)
		{
			if(each.isDirectory() || each.isHidden())
				continue;
			HDFSFile file =new HDFSFile(each.getName(),foldername);
			System.out.println("find "+localFolderName+"/"+each.getName());
			file.createFrom(localFolderName+"/"+file.filename);
			files.put(file.filename, file);
		}
		return "ok";
	}
	public String moveTo(String localFilePath) {
		for(String each: files.keySet())
		{
			HDFSFile hold = files.get(each);
			hold.moveTo(localFilePath);
		}
		return "ok";
	}
}
