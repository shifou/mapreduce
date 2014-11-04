package main;

import java.rmi.RemoteException;

import hdfs.NameNode;

public class Master {
	public static void main(String[] args) {
		try {
		 Environment.configure();
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("please configure hdfs and mapred first");
			
			System.exit(1);
		}
		NameNode nameNode = new NameNode(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
		try {
			nameNode.start();
		} catch (RemoteException e) {
			System.err.println("Namenode cannot export and bind its remote object.\n");
			System.exit(1);
		}
	}
}
