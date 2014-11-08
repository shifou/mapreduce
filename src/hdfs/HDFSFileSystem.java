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
			File f=new File(localFileName);
			if(f.exists()==false)
				return "this file does not exist in the local filesystem";
			if(f.isDirectory())
				return "can not put the directory to hdfs using put";
			HDFSFile file =new HDFSFile(hdfsFileName,null);
			String ans = file.createFrom(localFileName);
			fileList.put(hdfsFileName,file);
			return ans;
	}
	public String putFolder(String localFolderName, String hdfsFolderPath) {
			File f=new File(localFolderName);
			if(f.isDirectory()==true && f.exists()==false)
				return "this folder does not exist in the local filesystem";
			if(f.isDirectory()==false)
				return "can not put file to hdfs using putr";
			HDFSFolder folder = new HDFSFolder(localFolderName);
			String ans = folder.createFrom(localFolderName);
			folderList.put(hdfsFolderPath,folder);
			return ans;
	}
	
	
}
