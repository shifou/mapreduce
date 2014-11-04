package main;

import java.rmi.RemoteException;

import hdfs.DataNode;
import hdfs.NameNode;

public class Slave {
	public static void main(String[] args) {
		try {
		 Environment.configure();
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("please configure hdfs and mapred first");
			
			System.exit(1);
		}
		DataNode dataNode = new DataNode(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
		try {
			dataNode.start();
		} catch (RemoteException e) {
			System.err.println("Datanode cannot export and bind its remote object.\n");
			System.exit(1);
		}
	}
}
