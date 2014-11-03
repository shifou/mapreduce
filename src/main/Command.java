package main;

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
		
		if (!args[0].equals("hadoop")) {
			printUsage();
			return;
		}
		
		if (args[1].equals("put")) {
			if (args.length < 4) {
				printPutUsage();
				return;
			}
			String localFilePath = args[2];
			String hdfsFilePath  = args[3];
			putToHDFS(localFilePath, hdfsFilePath);
		} else if (args[1].equals("get")) {
			if (args.length < 4) {
				printGetUsage();
				return;
			}
			String localFilePath = args[3];
			String hdfsFilePath  = args[2];
			getFromHDFS(hdfsFilePath, localFilePath);
		} else if (args[1].equals("rm")) {
			if (args.length < 3) {
				printRmUsage();
				return;
			}
			String hdfsFilePath = args[2];
			removeFromHDFS(hdfsFilePath);
		} else if (args[1].equals("ls")) {
			listFiles();
			return;
		} else if (args[1].equals("hdfs")) {
			
			if (args[2].equals("lsft")) { //List NameNode File Table
				printNameNodeFileTbl();
			}
			
		}
		
		else if (args[1].equals("mapred")) {
			mapredUtility(args);
		} else {
			printUsage();
			return;
		}
	}

private static void readConf() {
	// TODO Auto-generated method stub
	
}
	
}
