package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import main.Environment;

public class NameNode implements NameNodeRemoteInterface {
	private int port;
	public int dataNodeAssignId;
	public static PriorityBlockingQueue<DataNodeInfo> load;
	private NameNodeRemoteInterface nameNodeStub;
	public static HDFSFileSystem fileSystem;
	public static ConcurrentHashMap<String, DataNodeInfo> cluster;
	

	public NameNode(int port) {
		Environment.createDirectory("");
		fileSystem = new HDFSFileSystem();
		cluster = new ConcurrentHashMap<String, DataNodeInfo>();
		dataNodeAssignId = 1;
		load= new PriorityBlockingQueue<DataNodeInfo>();
		nameNodeStub = null;
		this.port = port;
	}

	public boolean start() {

		if (Environment.createDirectory(Environment.Dfs.NAMENODE_SERVICENAME) == false)
			return false;
		Registry registry = null;
		try {
			registry = LocateRegistry.createRegistry(this.port);
			this.nameNodeStub = (NameNodeRemoteInterface) UnicastRemoteObject
					.exportObject(this, 0);
			registry.rebind(Environment.Dfs.NAMENODE_SERVICENAME, nameNodeStub);
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public synchronized String handler(hdfsOP tp,String f1,String f2,DataNodeInfo slave)
	{
		try{
		switch(tp)
		{
		case DEL:
			return delete(f1);
		case DELR:
			return deleteR(f1);
		 case PUT:
			return copyFromLocal(f1,f2);
			case PUTR:
			return copyFromLocalR(f1,f2);
		case GET:
			return copyToLocal(f1,f2);
			case GETR:
			return copyToLocalR(f1,f2);
		case LIST:
			return list();
		case REPLICA:
			return handlerRecovery(slave);
		case JOIN:
			return join(f1);
		default:
			return "wrong operation received!";
		}
		}catch(Exception e)
		{
			e.printStackTrace();
			return "execute: "+tp+" error!";
		}
	}
	@Override
	public String delete(String filename) throws RemoteException, IOException {
		if(fileSystem.fileList.containsKey(filename)==false)
			return "file not exist in hdfs";
		String ans = fileSystem.deleteFile(filename);
		return ans;
	}
	public String deleteR(String foldername) throws RemoteException, IOException {
		if(fileSystem.folderList.containsKey(foldername)==false)
			return "file folder not found";
		String ans = fileSystem.deleteFolder(foldername);
		return ans;
	}
	public String copyFromLocalR(String localFolderName, String hdfsFolderPath) throws RemoteException{
		if(hdfsFolderPath.indexOf("/")!=-1)
			return "folder format error";
		if(cluster.size()<Environment.Dfs.REPLICA_NUMS)
		{
			return "can not copy because replica number greater than slaves\n";
		}
		if(fileSystem.folderList.containsKey(localFolderName))
		{
			return "folder duplicate already exist\n";
		}
		String ans = fileSystem.putFolder(localFolderName, hdfsFolderPath);
		return ans;
		
	}
	public String copyFromLocal(String localFileName, String hdfsFileName) throws RemoteException{
		if(hdfsFileName.indexOf("/")!=-1)
			return "please input the new filename you want to store as instead of the folder";
		if(cluster.size()<Environment.Dfs.REPLICA_NUMS)
		{
			return "can not copy because replica number greater than slaves\n";
		}
		if(fileSystem.fileList.containsKey(localFileName))
		{
			return "file duplicate already exist\n";
		}
		String ans = fileSystem.putFile(localFileName, hdfsFileName);
		return ans;
	}

	public String copyToLocalR(String hdfsFilePath, String localFilePath) throws RemoteException
	{
		if(fileSystem.folderList.containsKey(hdfsFilePath))
			return "folder duplicate in the hdfs";
		else
		{
			String ans = fileSystem.getFolder(hdfsFilePath,localFilePath);
			return ans;
		}
	}
	public String copyToLocal(String hdfsFilePath, String localFilePath) throws RemoteException
	{
		if(fileSystem.fileList.containsKey(hdfsFilePath)==false)
			return "no such file in the hdfs";
		else
		{
			String ans = fileSystem.getFiles(hdfsFilePath,localFilePath);
			return ans;
		}
	}

	public String replica(DataNodeInfo slave)
	{
		if(slave.lostTime==0)
			load.remove(slave);
		return fileSystem.ReAllocate(slave);
	}
	public String handlerRecovery(DataNodeInfo slave) {
		return handler(hdfsOP.REPLICA,null,null,slave);
	}
	public static String findIp(String name){
		if(cluster.containsKey(name))
		{
			return cluster.get(name).ip;
		}
		else 
			return "";
	}
	@Override
	public String list() throws RemoteException{
		String ans = "";
		int ff = 0,fd=0;
		if (fileSystem.fileList.isEmpty()&&fileSystem.folderList.isEmpty())
			return "there is no file and folder yet";
		for (String file : fileSystem.fileList.keySet()) {
			ans += (file + "\n");
			ff++;
		}
		for(String folder: fileSystem.folderList.keySet())
		{
			ans+=(folder+"\n");
			for(String one: fileSystem.folderList.get(folder).files.keySet()){
				ans+=("----"+one+"\n");
				ff++;
			}
			fd++;
		}
		ans+=("total files: "+ff+"\t"+"total folder: "+fd+"\n");
		return ans;
	}

	@Override
	public void quit() throws RemoteException{
		//.cleanup(Environment.Dfs.NAMENODE_SERVICENAME);
	}

	@Override
	public String join(String ip) throws RemoteException{

		String ans = "d" + this.dataNodeAssignId;
		DataNodeInfo one = null;
		one = new DataNodeInfo(ip, ans,Environment.TIME_LIMIT);
		this.cluster.put(ans, one);
		load.put(one);
		System.out.println("one slave join in with ip "+ip+" get id: " + dataNodeAssignId);
		dataNodeAssignId++;
		return ans;
	}
	public static Vector<DataNodeInfo> select(int nums)
	{
		Vector<DataNodeInfo> ans=new Vector<DataNodeInfo>();
		DataNodeInfo hold=null;
		int i=0;
		System.out.println("now cluster:"+load.size());
			while(load.isEmpty()==false&&i<nums)
			{
				i++;
				hold=load.poll();
				hold.blockload++;
				ans.add(hold);
			}
			for(DataNodeInfo temp : ans )
				load.add(temp);
		System.out.print("select: \n");
		for(DataNodeInfo k:ans)
			System.out.println(k.serviceName+"\t"+k.ip);
		return ans;
	}

	public static String replica_select(HashSet<String> existing) {
		Vector<DataNodeInfo> tp=new Vector<DataNodeInfo>();	
		DataNodeInfo hold=null;
		String ans="";
			while(load.isEmpty()==false)
			{
				hold=load.poll();
				if(existing.contains(hold.serviceName)==false)
				{
					hold.blockload++;
					ans=hold.serviceName;
				}
				tp.add(hold);
			}
			for(DataNodeInfo temp : tp )
				load.add(temp);
		return ans;
	}
}
