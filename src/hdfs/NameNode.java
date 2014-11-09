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
	public ConcurrentHashMap<String, DataNodeInfo> cluster;
	
	// slaveCheck systemCheck;

	public NameNode(int port) {
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
		if(fileSystem.fileList.containsKey(filename))
			return "file duplicate in hdfs";
		String ans = fileSystem.deleteFile(filename);
		return ans;
	}
	public String deleteR(String foldername) throws RemoteException, IOException {
		if(fileSystem.folderList.containsKey(foldername)==false)
			return "file folder not found";
		String ans = fileSystem.deleteFolder(foldername);
		return ans;
	}
	public String copyFromLocalR(String localFolderName, String hdfsFolderPath) {
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
	public String copyFromLocal(String localFileName, String hdfsFileName) {
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

	public String copyToLocalR(String hdfsFilePath, String localFilePath) 
	{
		if(fileSystem.folderList.containsKey(hdfsFilePath))
			return "folder duplicate in the hdfs";
		else
		{
			String ans = fileSystem.getFolder(hdfsFilePath,localFilePath);
			return ans;
		}
	}
	public String copyToLocal(String hdfsFilePath, String localFilePath) 
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
	@Override
	public String list() {
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
			for(String one: fileSystem.folderList.get(folder).files.keySet())
				ans+=("----"+one+"\n");
		}
		return ans;
	}

	@Override
	public String heart(int slaveId) {
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
	public void quit() {
		//.cleanup(Environment.Dfs.NAMENODE_SERVICENAME);
	}

	@Override
	public String join(String ip) {

		String ans = "d" + this.dataNodeAssignId;
		DataNodeInfo one = null;
		one = new DataNodeInfo(ip, ans);
		this.cluster.put(ans, one);
		load.put(one);
		Master.slaveStatus.put(dataNodeAssignId, Environment.TIME_LIMIT);
		System.out.println("one slave join in get id: " + dataNodeAssignId);
		dataNodeAssignId++;
		return ans;
	}
	public static List<String> select(int nums)
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
