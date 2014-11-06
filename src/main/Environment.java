package main;

import java.io.File;

public class Environment {
	public static class Dfs 
	{
		public static String NAME_NODE_IP;
		public static String NAMENODE_SERVICENAME;
		public static int NAME_NODE_REGISTRY_PORT;
		public static int DATA_NODE_REGISTRY_PORT;
		public static String DIRECTORY;
		public static int NAME_NODE_CHECK_PERIOD;
		public static int REPLICA_NUMS;
	}

	public static boolean configure() {
		// TODO Auto-generated method stub
		
	}

	public static boolean createDirectory() {
		
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
	

}
