package main;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class Command {
public static void main(String[] args) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		try {
			readConf();
		} catch (Exception e) {
			
			e.printStackTrace();
			
			System.err.println("Cannot read configuration info.\n"
					+ "Please confirm the hdfs.xml is placed as ./conf/hdfs.xml.\n");
			
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
private static void listHandler() {
	// TODO Auto-generated method stub
	
}
private static void deleteHandler(String hdfsFilePath) {
	// TODO Auto-generated method stub
	
}
private static void getHandler(String hdfsFilePath, String localFilePath) {
	// TODO Auto-generated method stub
	
}
private static void putHandler(String localFilePath, String hdfsFilePath) {
	// TODO Auto-generated method stub
	
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

private static void readConf() {
	// TODO Auto-generated method stub
	
}
	
}
