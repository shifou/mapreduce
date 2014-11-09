package hdfs;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class HDFSBlock implements Serializable {
	
	private String blockFileName;
	private int ID; 
	private ConcurrentHashMap<Integer, String> repIDtoLoc;
	private String blockFolderName;
	/**
	 * 
	 */
	private static final long serialVersionUID = -3110335214705117456L;
	
	public HDFSBlock(String blockFileName, int ID, Byte[] data, int blockSize, List<String> locations, String folderName){
		this.blockFileName = blockFileName;
		this.blockFolderName = folderName;
		this.ID = ID;
		this.repIDtoLoc = new ConcurrentHashMap<Integer, String>();
		for (int i = 0; i < locations.size(); i++){
			this.repIDtoLoc.put(i, locations.get(i));
		}
		try {
			Registry reg = LocateRegistry.getRegistry(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			for (Integer repID : this.repIDtoLoc.keySet()){
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(repID));
				dataNodeStub.putFile(data, blockSize, this);
			}
		} catch (RemoteException | NotBoundException e) {
			
			e.printStackTrace();
		}
		
	}

	public static String getIp() {
		
		return null;
	}

	public static int getPort() {
		
		return 0;
	}

	public static String getServiceName() {
		
		return null;
	}
	
	public String getFileName(){
		return this.blockFileName;
	}
	
	public String getFolderName(){
		return this.blockFolderName;
	}
	
	public int getID(){
		return this.ID;
	}

	public boolean delete(){
		
		try {
			Registry reg = LocateRegistry.getRegistry(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			for (Integer repId: this.repIDtoLoc.keySet()){
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(repId));
				String fullPath;
				if (this.blockFolderName != null){
					fullPath = Environment.Dfs.DIRECTORY+"/"+this.blockFolderName+"/"+this.blockFileName+"."+this.ID;
				}
				else {
					fullPath = Environment.Dfs.DIRECTORY+"/"+this.blockFileName+"."+this.ID;
				}
				if (dataNodeStub.delete(fullPath, this.ID) == false){
					return false;
				}
			}
			return true;
		} catch (RemoteException | NotBoundException e) {
			e.printStackTrace();
			return false;
			
		} 
	}
	
	public int get(byte[] data){
		try {
			Registry reg = LocateRegistry.getRegistry(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			for (Integer repId: this.repIDtoLoc.keySet()){
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(repId));
				Byte[] b = dataNodeStub.getFile(this);
				if (b != null){
					int len = b.length;
					for (int i = 0; i < len; i++){
						data[i] = b[i];
					}
					return len;
				}
			}
		} catch (RemoteException | NotBoundException e){
			e.printStackTrace();
			return -1;
		}
		return -1;
	}
	

}
