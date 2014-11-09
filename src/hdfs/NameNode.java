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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import main.Environment;
import main.Master;
import data.Message;

public class NameNode implements NameNodeRemoteInterface {
	private int port;
	public int dataNodeAssignId;
	public static PriorityBlockingQueue<DataNodeInfo> load;
	private NameNodeRemoteInterface nameNodeStub;
	public HDFSFileSystem fileSystem;
	public static ConcurrentHashMap<String, DataNodeInfo> cluster;
	
	// slaveCheck systemCheck;

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

	
	public static int handlerRecovery(int slaveId) {
		return 1;
		
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
	public String heart(int slaveId) throws RemoteException{
		if(cluster.containsKey("d"+slaveId)==false)
		{
			System.out.println("slave "+slaveId+" turn to alive");
			return "join";
		}
		else
		{
			Master.slaveStatus.put(slaveId,Master.slaveStatus.get(slaveId)+1);
			return "ok";
		}
	}
	@Override
	public void quit() throws RemoteException{
		//.cleanup(Environment.Dfs.NAMENODE_SERVICENAME);
	}

	@Override
	public String join(String ip) throws RemoteException{

		String ans = "d" + this.dataNodeAssignId;
		DataNodeInfo one = null;
		one = new DataNodeInfo(ip, ans);
		this.cluster.put(ans, one);
		load.put(one);
		Master.slaveStatus.put(dataNodeAssignId, Environment.TIME_LIMIT);
		System.out.println("one slave join in with ip "+ip+" get id: " + dataNodeAssignId);
		dataNodeAssignId++;
		return ans;
	}
	public static List<DataNodeInfo> select(int nums)
	{
		List<DataNodeInfo> ans=new ArrayList<DataNodeInfo>();
		DataNodeInfo hold=null;
		int i=0;
		System.out.println("now cluster:"+load.size());
		synchronized(load)
		{
			while(load.isEmpty()==false&&i<nums)
			{
				i++;
				hold=load.poll();
				hold.blockload++;
				ans.add(hold);
			}
			for(DataNodeInfo temp : ans )
				load.add(temp);
		}
		System.out.print("select: \n");
		for(DataNodeInfo k:ans)
			System.out.println(k.serviceName+"\t"+k.ip);
		return ans;
	}
}
