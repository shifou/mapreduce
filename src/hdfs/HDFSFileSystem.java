package hdfs;

import hdfs.NameNode.Node;

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
		HDFSFile file = this.fileList.get(filename);
		String ans = file.delete();
		fileList.remove(filename);
		return ans;
	}
	public String deleteFolder(String foldername)
	{
		HDFSFolder folder = folderList.get(foldername);
		String ans = folder.delete();
		folderList.remove(foldername);
		return ans;
	}
	public String getFolder(String hdfsFilePath, String localFilePath) {
		File f = new File(localFilePath);
		if(f.isDirectory())
			return "can not delete directory";
		if(f.exists())
			return "file already in local filesystem, please type in another filename";
		HDFSFile hdfsfile = dfs.get(hdfsFilePath);
		FileOutputStream out = null;
		out =  new FileOutputStream(localFilePath);
		int c;
		int counter = 0;
		byte[] buff = new byte[Environment.Dfs.BUF_SIZE];
		for(int i=0;i<hdfsfile.getBlockSize();i++)
		{
			c = hdfsfile.getBlock(buff,i);
			if(c==-1)
				return "get file failed due to too many node crush and can not get a complete file";
			out.write(buff, 0, c);
			counter += c;
		}
		out.close();
		System.out.println("READ: " + counter);
	}
	public String getFiles(String hdfsFilePath, String localFilePath) {
		File f = new File(localFilePath);
		if(f.isDirectory())
			return "can not delete directory";
		if(f.exists())
			return "file already in local filesystem, please type in another filename";
		HDFSFile hdfsfile = dfs.get(hdfsFilePath);
		FileOutputStream out = null;
		out =  new FileOutputStream(localFilePath);
		int c;
		int counter = 0;
		byte[] buff = new byte[Environment.Dfs.BUF_SIZE];
		for(int i=0;i<hdfsfile.getBlockSize();i++)
		{
			c = hdfsfile.getBlock(buff,i);
			if(c==-1)
				return "get file failed due to too many node crush and can not get a complete file";
			out.write(buff, 0, c);
			counter += c;
		}
		out.close();
		System.out.println("READ: " + counter);
	
	}
	public String putFile(String localFileName, String hdfsFileName) {
			File f=new File(localFileName);
			if(f.exists()==false)
				return "this file does not exist in the local filesystem";
			if(f.isDirectory())
				return "can not put the directory to hdfs using put";
			HDFSFile file =new HDFSFile(hdfsFileName);
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
			
		return "success!\n";
	}
	
	
}
