package hdfs;

import hdfs.NameNode.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import main.Environment;

public class HDFSFileSystem {
	public ConcurrentHashMap<String, HDFSFolder> folderList;
	public ConcurrentHashMap<String, HDFSFile> fileList;
	public PriorityBlockingQueue<DataNodeInfo> load;
	public HDFSFileSystem(){
		folderList=new  ConcurrentHashMap<String, HDFSFolder>();
		fileList = new  ConcurrentHashMap<String, HDFSFile>();
		load= new PriorityBlockingQueue<DataNodeInfo>();
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
		try {
			File f=new File(localFilePath);
			if(f.isDirectory())
				return "can not put the directory to hdfs";
			FileInputStream in = new FileInputStream(localFilePath);
			int c = 0;
			HDFSFile file = new HDFSFile(localFilePath);
			int blocksize=0;
			byte[] buff = new byte[Environment.Dfs.BUF_SIZE];
			while ((c = in.read(buff)) != -1) {
				List<String> locations = select(Environment.Dfs.REPLICA_NUMS);
				if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
				{
					return "Abondon put task Reason: can not fulfil replica nums during putting the block\n";
				}
				int id=0;
				Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
				for(byte b: buff)
					   data[id++] = b;
				file.addBlock(data, blocksize, c,locations);
				blocksize++;
			}
			in.close();
			this.dfs.put(localFilePath, file);
		} catch (Exception e) {
			e.printStackTrace();
			return "Error! Failed to put file to HDFS.";
		}
	}
	public String putFolder(String localFolderName, String hdfsFolderPath) {
		try {
			File f=new File(localFilePath);
			if(f.isDirectory())
				return "can not put the directory to hdfs";
			FileInputStream in = new FileInputStream(localFilePath);
			int c = 0;
			HDFSFile file = new HDFSFile(localFilePath);
			int blocksize=0;
			byte[] buff = new byte[Environment.Dfs.BUF_SIZE];
			while ((c = in.read(buff)) != -1) {
				List<String> locations = select(Environment.Dfs.REPLICA_NUMS);
				if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
				{
					return "Abondon put task Reason: can not fulfil replica nums during putting the block\n";
				}
				int id=0;
				Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
				for(byte b: buff)
					   data[id++] = b;
				file.addBlock(data, blocksize, c,locations);
				blocksize++;
			}
			in.close();
			this.dfs.put(localFilePath, file);
		} catch (Exception e) {
			e.printStackTrace();
			return "Error! Failed to put file to HDFS.";
		}
		return "success!\n";
	}
	public List<String> select(int nums)
	{
		List<String> res=null;
		List<DataNodeInfo> ans=null;
		DataNodeInfo hold;
		int i=0;
		synchronized(load)
		{
			while(load.isEmpty()==false&&i<nums)
			{
				i++;
				hold=load.poll();
				hold.blockload++;
				ans.add(hold);
				res.add(hold.serviceName);
			}
			for(DataNodeInfo temp : ans )
				load.add(temp);
		}
		return res;
	}
	
}
