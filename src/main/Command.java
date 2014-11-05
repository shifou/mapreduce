package main;

import hdfs.NameNodeRemoteInterface;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Command {

public static void main(String[] args) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		if(checkConf()==false) {
			System.err.println("framework configuration error.\n"
					+ "Please configure the hdfs.xml and mapred.xml first and restart hadoop\n");
			System.exit(1);
		} 
		
		if (args.length < 2) {
			printUsage();
			return;
		}
		
		if (args[0].equals("hadoop")==false && args[0].equals("dfs")==false) {
			printUsage();
			return;
		}
		if(args[0].equals("dfs"))
		{
		if (args[1].equals("put")) {
			if (args.length != 4) {
				printPutUsage();
				return;
			}
			String localFilePath = args[2];
			String hdfsFilePath  = args[3];
			putHandler(localFilePath, hdfsFilePath);
		} else if (args[1].equals("get")) {
			if (args.length != 4) {
				printGetUsage();
				return;
			}
			String localFilePath = args[3];
			String hdfsFilePath  = args[2];
			getHandler(hdfsFilePath, localFilePath);
		} 
		else if (args[1].equals("rm")) {
			if (args.length != 3) {
				printRmUsage();
				return;
			}
			String hdfsFilePath = args[2];
			deleteHandler(hdfsFilePath);
		} 
		else if (args[1].equals("ls"))
			listHandler();
		}
		else
		{
			// for mapreduce
			
		}
	}
private static boolean checkConf() {
	// TODO Auto-generated method stub
	return true;
}
private static void listHandler() {
	try {
		Registry nameNodeRegistry = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_IP, Environment.Dfs.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		String ans = nameNodeStub.listFiles();
		System.out.println(ans);
		} catch (RemoteException e){
			System.out.println("list failed");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Name node cant find");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("File Error");
			System.exit(-1);
		}
		
}
private static void deleteHandler(String hdfsFilePath) {
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
private static void getHandler(String hdfsFilePath, String localFilePath) {
	try {
	Registry nameNodeRegistry = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_IP, Environment.Dfs.NAME_NODE_REGISTRY_PORT);
	NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
	String ans = nameNodeStub.copyToLocal(hdfsFilePath,localFilePath);
	System.out.println(ans);
	} catch (RemoteException e){
		System.out.println("copyToLocal failed");
		System.exit(-1);
	} catch (NotBoundException e) {
		System.out.println("Name node cant find");
		System.exit(-1);
	} catch (IOException e) {
		System.out.println("File Error");
		System.exit(-1);
	}
	
}
private static void putHandler(String localFilePath, String hdfsFilePath) {
	try {
		Registry nameNodeRegistry = LocateRegistry.getRegistry(Environment.Dfs.NAME_NODE_IP, Environment.Dfs.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		String ans = nameNodeStub.copyFromLocal(localFilePath,hdfsFilePath);
		System.out.println(ans);
		} catch (RemoteException e){
			System.out.println("copyFromLocal failed");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Name node cant find");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("File Error");
			System.exit(-1);
		}		
}
// reminder: add -r option later
private static void printRmUsage() {
	System.out.println("Usage: java Command hadoop rm <filename>");
}

private static void printGetUsage() {
	System.out.println("Usage: java Command hadoop get <hdfs file> <local path>");
}

private static void printPutUsage() {
	System.out.println("Usage: hadoop put <local file> <hdfs path>");
}

private static void printUsage() {
	System.out.format("Usage1: java Command dfs put|get|rm|ls <files>\nUsage2: java Command hadoop lsJob|submit|kill\n");

}	
}
