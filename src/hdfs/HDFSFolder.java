package hdfs;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class HDFSFolder{
	public int fileSize;
	public String foldername;
	public ConcurrentHashMap<String, HDFSFile> files;
	public HashSet<String> folderInSlave;
	public HDFSFolder(String localFolderName) {
		foldername=localFolderName;
		files =new ConcurrentHashMap<String, HDFSFile> ();
		folderInSlave= new HashSet<String>();
	}
	public int filesize() {
		// TODO Auto-generated method stub
		return fileSize;
	}
	public String delete() {
		String res="";
		for(String name:files.keySet())
		{
			HDFSFile hold = files.get(name);
			System.out.println(hold.delete());
		}
		for(String name:folderInSlave)
		{
			String ipaddr= NameNode.findIp(name);
			Registry reg;
			try {
				reg = LocateRegistry.getRegistry(ipaddr, Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(name);
				res+=(dataNodeStub.deleteFolder(this.foldername)+"\n");
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return foldername;
	}
	public String createFrom(String localFolderName) {
		File f=new File(localFolderName);
		File [] ff= f.listFiles();
		System.out.println("copy from folder "+ localFolderName);
		String res="";
		for(File each : ff)
		{
			if(each.isDirectory() || each.isHidden())
				continue;
			HDFSFile file =new HDFSFile(each.getName(),foldername);
			System.out.println("find "+localFolderName+"/"+each.getName());
			HashSet<String> ans=file.createFrom(localFolderName+"/"+file.filename);
			if(ans.contains("#")==true)
			{
				ans.remove("#");
				for(String tt:ans)
					res+=tt;
				return res;
			}
			else
			{
				for(String tt:ans)
				{
					if(res.charAt(0)=='?')
					res+=(tt+"\n");
					else
						folderInSlave.add(tt);
				}
			files.put(file.filename, file);
			}
		}
		return res;
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
