package hdfs;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;
import data.Message;

public class NameNode implements NameNodeRemoteInterface {
	private int port;
	public int dataNodeAssignId;
	private NameNodeRemoteInterface nameNodeStub;
	public ConcurrentHashMap<String, HDFSFile> dfs;
	//slaveCheck systemCheck;

	public NameNode(int port) {
		dfs =new ConcurrentHashMap<String, HDFSFile>();
		dataNodeAssignId = 0;
		this.port = port;
	}

	public boolean start() {

		if (Environment.createDirectory())
			return false;
		Registry registry = null;
		try {
			registry = LocateRegistry.createRegistry(this.port);
			this.nameNodeStub = (NameNodeRemoteInterface) UnicastRemoteObject
					.exportObject(this, 0);
			registry.rebind("NameNode", nameNodeStub);
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		}
		systemCheck = new slaveCheck(0);
		Thread ck = new Thread(systemCheck);
		ck.start();
		return true;
	}
	/*
	private class slaveCheck implements Runnable {
		private volatile boolean running;

		public slaveCheck(int i) {
			running = true;
		}

		public void run() {

			while (running) {
				check();
				try {
					Thread.sleep(Environment.Dfs.NAME_NODE_CHECK_PERIOD);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void check() {
		// TODO Auto-generated method stub

	}
	*/
	@Override
	public String delete(String path) throws RemoteException, IOException {
		HDFSFile file = this.dfs.get(path);
		for (HDFSBlock one : file.getBlockList()) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_IP, Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
			String ans = nameNodeStub.delete(hdfsFilePath);
			System.out.println(ans);
		} catch (RemoteException e){
			System.out.println("delete failed");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Name node cant find");
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

	@Override
	public void quit() {

		systemCheck.running = false;

	}
}
