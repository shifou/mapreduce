package hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import main.Environment;

public class HDFSFileSystem {
	public ConcurrentHashMap<String, HDFSFolder> folderList;
	public ConcurrentHashMap<String, HDFSFile> fileList;

	public HDFSFileSystem(){
		folderList=new  ConcurrentHashMap<String, HDFSFolder>();
		fileList = new  ConcurrentHashMap<String, HDFSFile>();
		}
	public String deleteFile(String filename)
	{
		if(fileList.containsKey(filename)==false)
			return "file does not exist";
		HDFSFile file = this.fileList.get(filename);
		String ans = file.delete();
		fileList.remove(filename);
		return ans;
	}
	public String deleteFolder(String foldername)
	{
		if(folderList.containsKey(foldername)==false)
			return "folder does not exist";
		HDFSFolder folder = folderList.get(foldername);
		String ans = folder.delete();
		folderList.remove(foldername);
		return ans;
	}
	public String getFolder(String hdfsFolderName, String localFilePath) {
		if(folderList.containsKey(hdfsFolderName)==false)
			return "folder does not exist";
		HDFSFolder hdfsfolder = folderList.get(hdfsFolderName);
		String ans = hdfsfolder.moveTo(localFilePath);
		return ans;
		
	}
	public String getFiles(String hdfsFileName, String localFilePath) {
		if(fileList.containsKey(hdfsFileName)==false)
			return "file does not exist";
		HDFSFile hdfsfile = fileList.get(hdfsFileName);
		String ans = hdfsfile.moveTo(localFilePath);
		return ans;
		
	}
	public String putFile(String localFileName, String hdfsFileName) {
			if(fileList.containsKey(hdfsFileName))
				return "this file duplicate in the hdfs filesystem";
			HDFSFile file =new HDFSFile(hdfsFileName,null);
			HashSet<String> ans = file.createFrom(localFileName);
			fileList.put(hdfsFileName,file);
			String res="";
			if(ans.contains("#")==true)
				ans.remove("#");
			for(String each:ans)
			{
				if(each.charAt(0)=='?'||each.charAt(0)=='#')
				res+=each;
			}
			return res;
	}
	public String putFolder(String localFolderName, String hdfsFolderPath) {
			if(folderList.containsKey(hdfsFolderPath))
				return "this folder duplicate in the hdfs filesystem";
			HDFSFolder folder = new HDFSFolder(hdfsFolderPath);
			String ans = folder.createFrom(localFolderName);
			folderList.put(hdfsFolderPath,folder);
			return ans;
	}
	
	
}
