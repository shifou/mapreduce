package main;


import hdfs.DataNode;


public class Slave {
	public static void main(String[] args) {
		System.out.println("start Slave");
		
		try {
			if( Environment.configure()==false)
			{
				System.err.println("please configure hdfs and mapred first");
				System.exit(1);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("please configure hdfs and mapred first");
			
			System.exit(1);
		}
		DataNode dataNode = new DataNode(Environment.Dfs.DATA_NODE_REGISTRY_PORT);
		dataNode.start();
	}
}
