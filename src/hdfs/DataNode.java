package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class DataNode implements DataNodeRemoteInterface{

	private int dataNodeRegistryPort;
	private DataNodeRemoteInterface dataNodeStub;
	private NameNodeRemoteInterface nameNodeStub;
	private ConcurrentHashMap<String, ConcurrentHashMap<Integer, HDFSBlock>> fileToBlock;
	private String serviceName;
	
	public DataNode(int dataNodeRegistryPort) {
		Environment.createDirectory("");
		this.dataNodeRegistryPort = dataNodeRegistryPort;
		this.fileToBlock = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, HDFSBlock>>();
	}

	public boolean start() {
		

		Registry reg;
		try {
			reg = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_IP, Environment.Dfs.NAME_NODE_REGISTRY_PORT);

			this.nameNodeStub = (NameNodeRemoteInterface)reg.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			
			this.serviceName = this.nameNodeStub.join(InetAddress.getLocalHost().getHostAddress());
			if (!Environment.createDirectory(this.serviceName)){
				return false;
			}
			this.dataNodeStub = (DataNodeRemoteInterface)UnicastRemoteObject.exportObject(this, 0);
			Registry r = LocateRegistry.createRegistry(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			r.rebind(serviceName, this.dataNodeStub);
		} catch (RemoteException | NotBoundException | UnknownHostException e) {
			e.printStackTrace();
			return false;
		} 
		return true;
		
		
		
	}

	@Override
	public boolean delete(String path, int ID) throws RemoteException{

		File toDelete = new File(path);
		if (toDelete.delete()){
			ConcurrentHashMap<Integer, HDFSBlock> blocks = this.fileToBlock.get(path);
			if (blocks != null){
				blocks.remove(ID);
				if (blocks.isEmpty()){
					this.fileToBlock.remove(blocks);
				}
				return true;
			}
			else {
				return false;
			}
		}
		else {
			return false;
		}
		
		
	}

	@Override
	public void putFile(Byte[] data, int blockSize, HDFSBlock block) throws RemoteException{
		String fullPath;
		String folderName = block.getFolderName();
		if (folderName != null){
			File folder = new File(Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+folderName);
			folder.mkdir();
			fullPath = Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+folderName+"/"+block.getFileName()+"."+block.getID();
		}
		else {
			fullPath = Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+block.getFileName()+"."+block.getID();
		}
		System.out.println("trying to add block: "+fullPath);
		File file = new File(fullPath);
		System.out.println("Path written to: "+ fullPath);
		
		try {
			file.createNewFile();
			FileOutputStream out = new FileOutputStream(file);
			byte[] toPut = new byte[blockSize];
			for (int i = 0; i< blockSize; i++){
				toPut[i] = data[i];
			}
			out.write(toPut);
			out.close();
			if (this.fileToBlock.get(fullPath) != null){
				this.fileToBlock.get(fullPath).put(block.getID(), block);
			}
			else {
				ConcurrentHashMap<Integer, HDFSBlock> newFile = new ConcurrentHashMap<Integer, HDFSBlock>();
				newFile.put(block.getID(), block);
				this.fileToBlock.put(fullPath, newFile);
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
			
	}

	@Override
	public Byte[] getFile(HDFSBlock block) throws RemoteException{
		try {
			String fullPath;
			String folderName = block.getFolderName();
			if (folderName != null){
				fullPath = Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+folderName+"/"+block.getFileName()+"."+block.getID();
			}
			else {
				fullPath = Environment.Dfs.DIRECTORY+"/"+this.serviceName+"/"+block.getFileName()+"."+block.getID();
			}
			File file = new File(fullPath);
			FileInputStream in = new FileInputStream(file);
			int count = 0;
			byte[] buffer = new byte[Environment.Dfs.BUF_SIZE];
			count = in.read(buffer);
			in.close();
			Byte[] data = new Byte[count];
			for (int i = 0; i < count; i++){
				data[i] = buffer[i];
			}
			return data;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	

}
