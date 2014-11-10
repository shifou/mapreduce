package main;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import data.Message;
import data.msgType;
import hdfs.DataNodeInfo;
import hdfs.DataNodeRemoteInterface;
import hdfs.NameNode;
import hdfs.NameNodeRemoteInterface;

public class Master {
	public static Timer monitor;
	public static NameNode nameNode;
	public static void startTimer() {
		System.out.println("--heartbeat--");
		
		monitor = new Timer(true);
		TimerTask task = new TimerTask() {
			public void run() {
				checkAlive();
			}
		};
		monitor.schedule(task, 0, Environment.Dfs.NAME_NODE_CHECK_PERIOD);
		
	}
	private static void checkAlive() {
		// System.out.println("check: "+status.size());
		for (String name : NameNode.cluster.keySet()) {
			DataNodeInfo hh = NameNode.cluster.get(name);
			try
			{
				Registry dataNodeRegistry = LocateRegistry.getRegistry(
						hh.ip,
						Environment.Dfs.DATA_NODE_REGISTRY_PORT);
				DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry
						.lookup(name);
				Boolean b= hh.lostTime>0;
				dataNodeStub.healthCheck(b);
				
			if(hh.lostTime<=0)
			{
						System.out.println("slave "+name+"\t"+NameNode.findIp(name)+" disconnected but turn to alive will assign a new servicename when join");
						NameNode.cluster.remove(name);
			}
			}catch (Exception e)
			{
				hh.lostTime--;
				NameNode.cluster.put(name,hh);
			}
			if (NameNode.cluster.get(name).lostTime == 0) {
				System.out.println("slave "+name+"\t"+NameNode.findIp(name)+" assumed disconnected, abondon all related tasks but try to connect in 5 times");
				System.out.println(handleLost(hh));
			}
			if(NameNode.cluster.get(name).lostTime==-5){
				System.out.println("slave "+name+"\t"+NameNode.findIp(name)+" disconnected, abondon all related tasks");
				NameNode.cluster.remove(name);
			}
		}
	}

	private static String handleLost(DataNodeInfo one) {
		String reply=NameNode.handlerRecovery(one);
		return reply;
		/*
		if(reply==Environment.Dfs.REPLICA_NUMS)
			return("recovery and still maintain the file replica successfully !");
		else if (reply>0)
			return("can recovery but can not maintain the file replica right now");
		else
			return("can not recovery the file and lost all files in slave "+one.serviceName+"\t"+one.ip);
		*/
	}
	public static void main(String[] args) {
		System.out.println("start Master");
		try {
		if( Environment.configure()==false)
		{
			System.out.println("please configure hdfs and mapred first");
			System.exit(1);
		}
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("please configure hdfs and mapred first");
			
			System.exit(1);
		}
		nameNode = new NameNode(Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			try {
				if(	nameNode.start()==false)
				{
					System.err.println("NameNode can not start");
					System.exit(-1);
				}
				else
					System.out.println("NameNode start successfully");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		startTimer();
	}
}
