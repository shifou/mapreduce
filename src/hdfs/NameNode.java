package hdfs;

import java.io.File;
import java.io.FileInputStream;
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
	private NameNodeRemoteInterface nameNodeStub;
	public ConcurrentHashMap<String, HDFSFile> dfs;
	public ConcurrentHashMap<String, Node> cluster;
	public PriorityBlockingQueue<Node> load;
	// slaveCheck systemCheck;

	public NameNode(int port) {
		dfs = new ConcurrentHashMap<String, HDFSFile>();
		cluster = new ConcurrentHashMap<String, Node>();
		
		dataNodeAssignId = 1;
		nameNodeStub = null;
		this.port = port;
	}

	public boolean start() {

		if (Environment.createDirectory() == false)
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
	public String delete(String path) throws RemoteException, IOException {
		HDFSFile file = this.dfs.get(path);
		dfs.remove(path);
		ConcurrentHashMap<Integer, HDFSBlock> fileblocks = file.getBlockList();

		for (Integer one : fileblocks.keySet()) {
			HDFSBlock hold = fileblocks.get(one);
			try {
				/*
				Registry nameNodeRegistry = LocateRegistry.getRegistry(
						HDFSBlock.getIp(), HDFSBlock.getPort());
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) nameNodeRegistry
						.lookup(HDFSBlock.getServiceName());
				String ans = dataNodeStub.delete(path);
				*/
				if(hold.delete()==false)
					System.out.println("notice some nodes fail when delete the block");
			} catch (RemoteException e) {
				System.out.println("delete failed");
				System.exit(-1);
			} catch (NotBoundException e) {
				System.out.println("data node cant find");
				System.exit(-1);
			} catch (IOException e) {
				System.out.println("File Error");
				System.exit(-1);
			}
		}
		return null;
	}

	@Override
	public String copyToLocal(String hdfsFilePath, String localFilePath) {
		if(cluster.containsKey(hdfsFilePath)==false)
			return "no such file in the hdfs";
		else
		{
			
		}
	}
	public List<Node> select(int nums)
	{
		List<Node> ans=null;
		Node hold;
		int i=0;
		synchronized(this.load)
		{
			while(load.isEmpty()==false&&i<nums)
			{
				i++;
				hold=load.poll();
				hold.blockload++;
				ans.add(hold);
			}
			for(Node temp : ans )
				load.add(temp);
		}
		return ans;
	}
	@Override
	public String copyFromLocal(String localFilePath, String hdfsFilePath) {
		if(cluster.size()<Environment.Dfs.REPLICA_NUMS)
		{
			return "can not copy because replica number greater than slaves\n";
		}
		if(dfs.containsKey(localFilePath))
		{
			return "file duplicate already exist\n";
		}
		byte[] buff = new byte[Environment.Dfs.BUF_SIZE];
		try {
			FileInputStream in = new FileInputStream(localFilePath);
			int c = 0;
			
			HDFSFile file = new HDFSFile(localFilePath);

			int blocksize=0;
			while ((c = in.read(buff)) != -1) {
				List<Node> locations = select(Environment.Dfs.REPLICA_NUMS);
				if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
				{
					return "Abondon put task Reason: can not fulfil replica nums during putting the block\n";
				}
				file.addBlock(buff, blocksize, c,locations);
			}
			in.close();
			this.dfs.put(localFilePath, file);
		} catch (Exception e) {
			e.printStackTrace();
			return "Error! Failed to put file to HDFS.";
		}
		return "success!\n";
	}

	@Override
	public String listFiles() {
		String ans = "";
		int id = 1;
		if (dfs.isEmpty())
			return "there is no file yet";
		for (String file : dfs.keySet()) {
			ans += (id + ": " + file + "\n");
		}
		return ans;
	}

	@Override
	public void quit() {

	}

	@Override
	public String join(String ip) {

		String ans = "d" + this.dataNodeAssignId;
		Node one = null;
		one = new Node(ip, ans);
		this.cluster.put(ans, one);
		Master.slaveStatus.put(dataNodeAssignId, 5);
		System.out.println("one slave join in get id: " + dataNodeAssignId);
		dataNodeAssignId++;
		return ans;
	}

	private class Node implements Comparable<Node>{

		public String serviceName;
		public String ip;
		public int blockload;
		private DataNodeRemoteInterface nodeService;

		public Node(String ip2, String ans) {
			ip = ip2;
			serviceName = ans;
			blockload=0;
			// Registry nodeRegistry = LocateRegistry.getRegistry(ip,
			// Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			// nodeService = (DataNodeRemoteInterface)
			// nodeRegistry.lookup(serviceName);
		}

		@Override
		public int compareTo(Node o) {
			return this.blockload-o.blockload;
		}
	}
}
