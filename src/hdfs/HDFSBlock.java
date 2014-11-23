package hdfs;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class HDFSBlock implements Serializable {
	
	private String blockFileName;
	private int ID; 
	public ConcurrentHashMap<Integer, DataNodeInfo> repIDtoLoc; 
	private String blockFolderName;

	private static final long serialVersionUID = -3110335214705117456L;
	
	public HDFSBlock(String blockFileName, int ID, Byte[] data, int blockSize, List<DataNodeInfo> locations, String folderName){
		this.blockFileName = blockFileName;
		this.blockFolderName = folderName;
		this.ID = ID;
		this.repIDtoLoc = new ConcurrentHashMap<Integer, DataNodeInfo>();
		for (int i = 0; i < locations.size(); i++){
			this.repIDtoLoc.put(i, locations.get(i));
		}
		try {
			
			for (Integer repID : this.repIDtoLoc.keySet()){
				Registry reg = LocateRegistry.getRegistry(this.repIDtoLoc.get(repID).ip,Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(repID).serviceName);
				dataNodeStub.putFile(data, blockSize, this);
			}
		} catch (RemoteException | NotBoundException e) {
			
			e.printStackTrace();
		}
		
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
			
			for (Integer repId: this.repIDtoLoc.keySet()){
				Registry reg = LocateRegistry.getRegistry(this.repIDtoLoc.get(repId).ip, Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(repId).serviceName);
				String fullPath;
				if (this.blockFolderName != null){
					fullPath = Environment.Dfs.DIRECTORY+"/"+this.repIDtoLoc.get(repId).serviceName+"/"+this.blockFolderName+"/"+this.blockFileName+"."+this.ID;
				}
				else {
					fullPath = Environment.Dfs.DIRECTORY+"/"+this.repIDtoLoc.get(repId).serviceName+"/"+this.blockFileName+"."+this.ID;
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
			for (Integer repId: this.repIDtoLoc.keySet()){
				Registry reg = LocateRegistry.getRegistry(this.repIDtoLoc.get(repId).ip, Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(repId).serviceName);
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
	
	public String newReplica(String oldServiceName){
		
		HashSet<String> existing=new HashSet<String>();
		int id = -1;
		for (Integer ID: this.repIDtoLoc.keySet()){
			
			if (this.repIDtoLoc.get(ID).serviceName.equals(oldServiceName)){
				this.repIDtoLoc.remove(ID);
				id = ID;
			}
			else {
				existing.add(this.repIDtoLoc.get(ID).serviceName);
			}
			
		}
		
		if (existing.isEmpty()){
			return "Can't be replicated, not enough nodes in the system!";
		}
		String newServiceName = NameNode.replica_select(existing);
		if(newServiceName.equals(""))
		{
			return "can not maintain replica any more!";	
		}
		byte[] data = new byte[Environment.Dfs.BUF_SIZE];
		int blockSize = this.get(data);
		Byte[] toPut = new Byte[Environment.Dfs.BUF_SIZE];
		
		for (int i = 0; i < blockSize; i++){
			toPut[i] = data[i];
		}
		int j = 0;
		
		while (!Character.isDigit(newServiceName.charAt(j))){
			j+=1;
		}
		String s = "";
		while (j < newServiceName.length()){
			s+= newServiceName.charAt(j);
			j+=1;
		}
		int slaveNum = Integer.parseInt(s);
		this.repIDtoLoc.put(id, new DataNodeInfo(NameNode.findIp(newServiceName), newServiceName, Environment.TIME_LIMIT, slaveNum));
		try{
			Registry reg = LocateRegistry.getRegistry(this.repIDtoLoc.get(id).ip,Environment.Dfs.DATA_NODE_REGISTRY_PORT);
			DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface)reg.lookup(this.repIDtoLoc.get(id).serviceName);
			dataNodeStub.putFile(toPut, blockSize, this);
		} catch (RemoteException | NotBoundException e) {
			
			e.printStackTrace();
			
		}
		return "ok#"+newServiceName;
		
	}
	
	public HashSet<Integer> getLocations(){
		HashSet<Integer> res = new HashSet<Integer>();
		for (Integer i : this.repIDtoLoc.keySet()){
			res.add((this.repIDtoLoc.get(i).slaveNum));
		}
		return res;
	}
	

}
