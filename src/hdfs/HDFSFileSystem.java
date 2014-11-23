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
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import main.Environment;
import main.Master;
import mapreduce.InputSplit;

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
				else
				{
					System.out.println(each+ " add file: "+hdfsFileName);
					Master.nameNode.cluster.get(each).files.add(hdfsFileName);
			
				}
			}
			return res;
	}
	public String putFolder(String localFolderName, String hdfsFolderPath) {
		HDFSFolder folder;	
		if(folderList.containsKey(hdfsFolderPath))
			folder = folderList.get(hdfsFolderPath);
		else
			folder = new HDFSFolder(hdfsFolderPath);
			String ans = folder.createFrom(localFolderName);
			folderList.put(hdfsFolderPath,folder);
			return ans;
	}
	public String ReAllocate(DataNodeInfo slave) {
		String res="";
		for(String name: slave.files)
		{
			if(fileList.containsKey(name)==false)
				continue;
			System.out.println("files with: "+name);
			HDFSFile hold = fileList.get(name);
			String ans = hold.Replica(slave.serviceName);
			String[] fk = ans.split("#");
			res+=(fk[0]+"\n");
			fileList.put(name, hold);
			if(fk.length>=2)
			{
				System.out.println("reallocate to "+fk[1]+" slaves");

				String []temp = fk[1].split(" ");
				for(int i=0;i<temp.length;i++)
				{
					Master.nameNode.cluster.get(temp[i]).files.add(name);
				}
			}
		}
		for(String name: slave.folders.keySet())
		{
			HDFSFolder hold = folderList.get(name);
			String ans = hold.Replica(slave.serviceName);
			res+=(ans+"\n");
			folderList.put(name, hold);
		}
		return res;
	}
	public InputSplit[] getSplit(String path) {
		InputSplit[] ans=null;
		if(this.folderList.containsKey(path)==false)
		{
				if(this.fileList.containsKey(path)==false)
					return null;
				else
				{
					HDFSFile file = fileList.get(path);
					ans= new InputSplit[file.blocks.size()];
					for(int i=0;i<file.blocks.size();i++)
						ans[i]=new InputSplit(1,file.blocks.get(i));
				}
		}
		else
		{
			HDFSFolder hold = folderList.get(path);
			int i=1;
			int ct=0;
			for(String name: hold.files.keySet())
			{
				HDFSFile file = hold.files.get(name);
				ct+=file.blocks.size();
			}
			ans= new InputSplit[ct];
			ct=0;
			for(String name: hold.files.keySet())
			{
				HDFSFile file = hold.files.get(name);
				for(int j=0;j<file.blocks.size();j++)
					ans[ct++]=new InputSplit(i,file.blocks.get(j));
				i++;
			}
		}
		return ans;
	}
	
	
	
}
