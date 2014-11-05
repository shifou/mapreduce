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
	public Timer monitor;
	public ConcurrentHashMap<Integer, Integer> slaveStatus;
	public NameNodeRemoteInterface nameNodeStub;
	public void startTimer() {
		System.out.println("--heartbeat--");
		monitor = new Timer(true);
		TimerTask task = new TimerTask() {
			public void run() {
				checkAlive();
			}
		};
		monitor.schedule(task, 0, 5000);
	}
	private void checkAlive() {

		ConcurrentHashMap<Integer, Integer> status = getSlaveStatus();
		// System.out.println("check: "+status.size());
		for (Integer one : status.keySet()) {
			int hh = status.get(one);
			if (status.get(one) == 0) {
				System.out.println("slave Id: " + one
						+ " disconnected, abondon all related tasks");
				remove(one);
				continue;
			}
			status.put(one, hh - 1);
			Message msg = new Message(msgType.HEART);
			send(one, msg);
		}
	}

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
			
	}
}
