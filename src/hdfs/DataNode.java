package hdfs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class DataNode  implements DataNodeRemoteInterface{

	private int dataNodeRegistryPort;
	private DataNodeRemoteInterface dataNodeStub;
	private NameNodeRemoteInterface nameNodeStub;
	private ConcurrentHashMap<String, HDFSBlock> fileToBlock;
	
	public DataNode(int dataNodeRegistryPort) {
		this.dataNodeRegistryPort = dataNodeRegistryPort;
		this.fileToBlock = new ConcurrentHashMap<String, HDFSBlock>();
	}

	public boolean start() {
		
		if (!Environment.createDirectory()){
			return false;
		}
		Registry reg;
		try {
			reg = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			this.nameNodeStub = (NameNodeRemoteInterface)reg.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String serviceName = this.nameNodeStub.join(InetAddress.getLocalHost().getHostAddress());
			this.dataNodeStub = (DataNodeRemoteInterface)UnicastRemoteObject.exportObject(this, 0);
			reg.rebind(serviceName, this.dataNodeStub);
		} catch (RemoteException | NotBoundException | UnknownHostException e) {
			e.printStackTrace();
			return false;
		} 
		return true;
		
		
		
	}

	@Override
	public String delete(String path) {
		
		HDFSBlock block = this.fileToBlock.get(path);
		if (block != null){
			block.delete();
		}
		
	}

}
