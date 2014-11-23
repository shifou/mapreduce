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
	public ConcurrentHashMap<String, HashSet<String>> slave2file;
	public HDFSFolder(String localFolderName) {
		foldername=localFolderName;
		files =new ConcurrentHashMap<String, HDFSFile> ();
		folderInSlave= new HashSet<String>();
		slave2file = new ConcurrentHashMap<String, HashSet<String>>();
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
			res+=(hold.delete()+"\n");
		}
		for(String name:folderInSlave)
		{
			System.out.println("folder in "+name);
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
			//folderInSlave.remove(name);
		}
		return res;
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
			
			if(files.containsKey(each.getName()))
			{
				res+=(each.getName()+" already put ignored\n");
				continue;
			}
			HDFSFile file =new HDFSFile(each.getName(),foldername);
			System.out.println("find "+localFolderName+"/"+each.getName());
			HashSet<String> ans=file.createFrom(localFolderName+"/"+file.filename);
			if(ans.contains("#")==true)
			{
				ans.remove("#");
				for(String tt:ans)
				{
					if(tt.charAt(0)=='#')
					{
						res+=(tt+'\n');
						return res;
					}
				}
			}
			else
			{
				for(String tt:ans)
				{
					if(tt.charAt(0)=='?')
					res+=(tt+"\n");
					else
					{
						folderInSlave.add(tt);
						ConcurrentHashMap<String,HashSet<String>> fk=NameNode.cluster.get(tt).folders;
						if(fk==null)
						{
							System.out.println("add "+tt+"\t"+foldername+"\t"+file.filename);
							NameNode.cluster.get(tt).folders=new ConcurrentHashMap<String,HashSet<String>>();
							NameNode.cluster.get(tt).folders.put(foldername, new HashSet<String>());
							NameNode.cluster.get(tt).folders.get(foldername).add(file.filename);
						}
						
						if(slave2file.containsKey(tt)==false)
						{
							HashSet<String> a=new HashSet<String>();
							a.add(each.getName());
							slave2file.put(tt, a);
						}
						else
							slave2file.get(tt).add(each.getName());
					}
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
	public String Replica(String serviceName) {
		String res="";
		HashSet<String> flist = slave2file.get(serviceName);
		for(String name: flist)
		{
			HDFSFile hold = files.get(name);
			String ans = hold.Replica(serviceName);
			String[]fk=ans.split("#");
			res+=(fk[0]+"\n");
			for(int i=1;i<fk.length;i++)
			{
			if(slave2file.containsKey(fk[i])==false)
			{
				HashSet<String> a=new HashSet<String>();
				a.add(name);
				slave2file.put(fk[i], a);
			}
			else
				slave2file.get(fk[i]).add(name);
			}
			files.put(name, hold);
		}
		slave2file.remove(serviceName);
		return res;
	}
}
