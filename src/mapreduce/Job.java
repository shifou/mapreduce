package mapreduce;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Date;

import main.Environment;

public class Job implements Serializable {

	private static final long serialVersionUID = -2872789904155820699L;
	public Configuration conf;
	private String jarName;
	private String jarPath;

	public Job(Configuration conf, String string) {
		// TODO Auto-generated constructor stub
	}

	public void setOutputKeyClass(Class class1) {
		// TODO Auto-generated method stub

	}

	public void setOutputValueClass(Class class1) {
		// TODO Auto-generated method stub

	}

	public void setMapperClass(Class class1) {
		// TODO Auto-generated method stub

	}

	public void setReducerClass(Class class1) {
		// TODO Auto-generated method stub

	}

	public void waitForCompletion(boolean b) {

		try {
			Registry reg = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			JobTrackerRemoteInterface jobTracker = (JobTrackerRemoteInterface) reg
					.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
			JobInfo info = jobTracker.submitJob(this);

			while (true) {
				Thread.sleep(5000);
				info = jobTracker.getJobStatus(info.getID());
				Date date = new Date();
				if (info.getStatus() == JobInfo.SUCCEEDED) {
					System.out.println(date.toString() + " Job " + info.getID()
							+ ": Completed Successfully!");
					break;
				} else if (info.getStatus() == JobInfo.FAILED) {
					System.out.println(date.toString() + " Job " + info.getID()
							+ ": Failed to complete!");
					break;
				} else if (info.getStatus() == JobInfo.RUNNING) {
					System.out.println(date.toString() + " Job " + info.getID()
							+ ": map " + info.getPrecentMapCompleted()
							+ " reduce " + info.getPrecentReduceCompleted());
				}
			}

		} catch (RemoteException | NotBoundException | InterruptedException e) {

			e.printStackTrace();
		}

	}

	public void setInputPath(Job job, String string) {
		// TODO Auto-generated method stub

	}

	public void setOutputPath(Job job, String string) {
		// TODO Auto-generated method stub

	}

	public void setOutputFormatClass(Class class1) {
		// TODO Auto-generated method stub

	}

	public void setInputFormatClass(Class class1) {
		// TODO Auto-generated method stub

	}

	public void setJarByPath(String path, String jarName) {
		this.jarName = jarName;
		this.jarPath = path;
	}

}
