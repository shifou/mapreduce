package main;

import java.rmi.RemoteException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import data.Message;
import data.msgType;
import hdfs.NameNode;
import hdfs.NameNodeRemoteInterface;

public class Master {
	public static Timer monitor;
	public static NameNode nameNode;
	public static ConcurrentHashMap<Integer, Integer> slaveStatus;
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
		for (Integer one : slaveStatus.keySet()) {
			int hh = slaveStatus.get(one);
			if (slaveStatus.get(one) == 0) {
				System.out.println("slave Id: " + one
						+ " disconnected, abondon all related tasks");
				//remove(one);
				handlerRecovery(one);
				continue;
			}
			slaveStatus.put(one, hh - 1);
			//Message msg = new Message(msgType.HEART);
			//send(one, msg);
		}
	}

	private static void handlerRecovery(Integer one) {
		int reply=NameNode.handlerRecovery(one);
		if(reply==Environment.Dfs.REPLICA_NUMS)
			System.out.println("recovery and still maintain the file replica successfully !");
		else if (reply>0)
			System.out.println("can recovery but can not maintain the file replica right now");
		else
			System.out.println("can not recovery the file and need to delete the file");
		
	}
	public static void main(String[] args) {
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
		//startTimer();
	}
}
