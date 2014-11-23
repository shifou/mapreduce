package main;

import hdfs.NameNodeRemoteInterface;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.JobTrackerRemoteInterface;

public class Command  {

	public static void main(String[] args) throws IllegalArgumentException,
			SecurityException, IOException, ClassNotFoundException,
			IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {

		if (Environment.configure() == false) {
			System.err
					.println("framework configuration error.\n"
							+ "Please configure the hdfs.xml and mapred.xml first and restart hadoop\n");
			System.exit(1);
		}

		if (args.length < 2) {
			printUsage();
			return;
		}

		if (args[0].equals("hadoop") == false && args[0].equals("dfs") == false) {
			printUsage();
			return;
		}
		if (args[0].equals("dfs")) {
			if (args[1].equals("put")) {
				if (args.length <3) {
					printPutUsage();
					return;
				}
				String localFilePath = args[2];
				String hdfsFilePath = "";
				if(args.length==3)
					hdfsFilePath = args[2];
				else
					hdfsFilePath = args[3];
				putHandler(localFilePath, hdfsFilePath);
			} else if (args[1].equals("putr")) {
				if (args.length <3) {
					printPutrUsage();
					return;
				}
				String localFilePath = args[2];
				String hdfsFilePath = "";
				if(args.length==3)
					hdfsFilePath = args[2];
				else
					hdfsFilePath = args[3];
				putRHandler(localFilePath, hdfsFilePath);
			} else if (args[1].equals("get")) {
				if (args.length != 4) {
					printGetUsage();
					return;
				}
				String localFilePath="";
				if(args.length==3)
					localFilePath= args[2];
				else
					localFilePath= args[3];
				String hdfsFilePath = args[2];
				getHandler(hdfsFilePath, localFilePath);
			} else if (args[1].equals("getr")) {
				if (args.length <3 ) {
					printGetrUsage();
					return;
				}
				String localFilePath="";
				if(args.length==3)
					localFilePath= args[2];
				else
					localFilePath= args[3];
				String hdfsFilePath = args[2];
				getRHandler(hdfsFilePath, localFilePath);
			} else if (args[1].equals("rm")) {
				if (args.length != 3) {
					printRmUsage();
					return;
				}
				String hdfsFilePath = args[2];
				deleteHandler(hdfsFilePath);
			} else if (args[1].equals("rmr")) {
				if (args.length != 3) {
					printRmrUsage();
					return;
				}
				String hdfsFilePath = args[2];
				deleteRHandler(hdfsFilePath);
			} else if (args[1].equals("ls"))
				listHandler();
			else {
				printDfsUsage();
				return;
			}

		} else if (args[0].equals("mapred")) {
			if (args[1].equals("submit")) {
				if(args.length!=6)
				{
					printSubmitUsage();
						return;
				}
				submitHandler(args[2],args[3],args[4],args[5]);
			} else if (args[1].equals("lsjob")) {
				listJobHandler();
			} else if (args[1].equals("killjob")) {
				if(args.length!=3)
				{
					printKillJobUsage();
					return;
				}
				killJobHandler(args[2]);
			} else {
				printMapReduceUsage();
				return;
			}

		} else {
			printUsage();
			return;
		}
	}

	private static void killJobHandler(String jobid) {
		// TODO Auto-generated method stub
		try {
			Registry jobRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.MapReduceInfo.JOBTRACKER_PORT);
			JobTrackerRemoteInterface jobNodeStub = (JobTrackerRemoteInterface) jobRegistry
					.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
			String ans = jobNodeStub.killJob(jobid);
			System.out.println(ans);
		} catch (RemoteException e) {
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


	private static void listJobHandler() {
		try {
			Registry jobRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.MapReduceInfo.JOBTRACKER_PORT);
			JobTrackerRemoteInterface jobNodeStub = (JobTrackerRemoteInterface) jobRegistry
					.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
			String ans = jobNodeStub.listjob();
			System.out.println(ans);
		} catch (RemoteException e) {
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

	private static void submitHandler(String Jarpath, String mainclass,
			String inpath, String outpath) {
		String jarpath = Jarpath;
		String mainClassName = mainclass;
		String intputPath = inpath;
		String outoutPath = outpath;
		

		JarFile jarFile;
		try {
			jarFile = new JarFile(jarpath);
			Enumeration<JarEntry> e = jarFile.entries();
			URL[] urls = { new URL("jar:file:" + jarpath +"!/") };
			ClassLoader cl = URLClassLoader.newInstance(urls);
			
			Class<?> mainClass = null;
			
			while (e.hasMoreElements()) {
	            
				JarEntry je = e.nextElement();
	            
				if(je.isDirectory() || !je.getName().endsWith(".class")){
	                continue;
	            }
	            
	            String className = je.getName().substring(0, je.getName().length() - 6);
	            className = className.replace('/', '.');
	            if (className.equals(mainClassName)) {
	            	mainClass =  cl.loadClass(className);
	            }
	        }
			if(mainClass==null)
			{
				System.out.println("can not find the main class in the jar\n");
				return;
			}
		    Method m = mainClass.getMethod("main", new Class[] {String[].class});
		    m.setAccessible(true);
		    
		    int mods = m.getModifiers();
		    
		    if (m.getReturnType() != void.class || !Modifier.isStatic(mods) ||
		        !Modifier.isPublic(mods)) {
		    	System.out.println("can not find the public static main class in the jar\n");
				return;
		    }
		    
		    String[] arggs = new String[4];
		    arggs[0]=inpath;
		    arggs[1]=outpath;
		    arggs[2]=jarpath;
		    arggs[3]=jarpath.substring(jarpath.indexOf("/")+1);
		    m.invoke(null, new Object[] { arggs });
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("error can not find the jar");
			return;
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("error when invoke the main");
			return;
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("error wrong arguments when invoke the main");
			return;
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("invoke failed");
			return;
		} catch (NoSuchMethodException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}



	private static void listHandler() {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub.list();
			System.out.println(ans);
		} catch (RemoteException e) {
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
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub.delete(hdfsFilePath);
			System.out.println(ans);
		} catch (RemoteException e) {
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

	private static void deleteRHandler(String hdfsFilePath) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub.deleteR(hdfsFilePath);
			System.out.println(ans);
		} catch (RemoteException e) {
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
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub.copyToLocal(hdfsFilePath, localFilePath);
			System.out.println(ans);
		} catch (RemoteException e) {
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

	private static void getRHandler(String hdfsFilePath, String localFilePath) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub.copyToLocalR(hdfsFilePath, localFilePath);
			System.out.println(ans);
		} catch (RemoteException e) {
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

	public static void putHandler(String localFilePath, String hdfsFilePath) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub
					.copyFromLocal(localFilePath, hdfsFilePath);
			System.out.println(ans);
		} catch (RemoteException e) {
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

	public static void putRHandler(String localFilePath, String hdfsFilePath) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry
					.lookup(Environment.Dfs.NAMENODE_SERVICENAME);
			String ans = nameNodeStub.copyFromLocalR(localFilePath,
					hdfsFilePath);
			System.out.println(ans);
		} catch (RemoteException e) {
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
	
	private static void printPutrUsage() {
		System.out.println("Usage: hadoop putr <local folder> <hdfs path>");

	}

	private static void printGetrUsage() {
		System.out
				.println("Usage: java XX.jar main.Command  dfs getr <hdfs folder> <local path>");
	}

	private static void printRmrUsage() {
		System.out.println("Usage: java XX.jar main.Command  dfs rmr <foldername>");
	}

	private static void printRmUsage() {
		System.out.println("Usage: java XX.jar main.Command  dfs rm <filename>");
	}

	private static void printGetUsage() {
		System.out
				.println("Usage: java XX.jar main.Command dfs get <hdfs file> <local path>");
	}

	private static void printPutUsage() {
		System.out.println("java XX.jar main.Command  dfs put <local file> <hdfs path>");
	}

	private static void printUsage() {
		System.out
				.format("Usage1: java XX.jar main.Command  dfs put|get|rm|ls <files>\nUsage2: java XX.jar main.Command  hadoop lsJob|submit|kill\n");

	}

	private static void printDfsUsage() {
		System.out
				.format("java XX.jar main.Command  dfs put|putr|getr|get|rm|rmr <files> <destPath>\n");

	}

	private static void printKillJobUsage() {
		System.out
		.format("java XX.jar main.Command  mapred <jobname>\n");
	
	}
	private static void printMapReduceUsage() {
		System.out
				.format("Usage: java XX.jar main.Command  mapred lsJob|submit|kill\n");

	}
	private static void printSubmitUsage() {
		// TODO Auto-generated method stub
		System.out.println("Usage: java XX.jar main.Command mapred submit <jarpath> <package.className> <inputpath> <outpath>\n");
	}

}
