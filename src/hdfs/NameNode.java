package hdfs;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import main.Environment;
import data.Message;

public class NameNode implements NameNodeRemoteInterface{
	private int port;
	private NameNodeRemoteInterface nameNodeStub;
	
	public NameNode(int port) {
		
		this.port = port;
	}
public boolean start() throws RemoteException {
		
		if(createDirectory())
			  return false;
		Registry registry = null;

		registry = LocateRegistry.createRegistry(this.port);
		this.nameNodeStub = (NameNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
		registry.rebind("NameNode", nameNodeStub);
		
		return true;
	}
private boolean createDirectory() {
	File folder = new File(Environment.Dfs.DIRECTORY);
	if (!folder.exists()) {
		if (folder.mkdir()) {
			System.out.println("Directory created");
		} else {
			System.err.println("Directory already used please change directory name or delete the directory first");
			return false;
		}
	}
	return true;
}
@Override
public Message heartBeat(int dataNodeId) throws RemoteException {
	// TODO Auto-generated method stub
	return null;
}
@Override
public String delete(String path) throws RemoteException, IOException {
	// TODO Auto-generated method stub
	return null;
}
@Override
public String copyToLocal(String hdfsFilePath, String localFilePath) {
	// TODO Auto-generated method stub
	return null;
}
@Override
public String copyFromLocal(String localFilePath, String hdfsFilePath) {
	// TODO Auto-generated method stub
	return null;
}
@Override
public String listFiles() {
	// TODO Auto-generated method stub
	return null;
}
}
