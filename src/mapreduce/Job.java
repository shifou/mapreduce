package mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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


	public JobInfo info;
	
	public Job(Configuration conf) {
		this.conf= conf;
		
	}

	public void waitForCompletion(boolean b) {

		try {
			Registry reg = LocateRegistry.getRegistry(
					Environment.Dfs.NAME_NODE_IP,
					Environment.Dfs.NAME_NODE_REGISTRY_PORT);
			JobTrackerRemoteInterface jobTracker = (JobTrackerRemoteInterface) reg
					.lookup(Environment.MapReduceInfo.JOBTRACKER_SERVICENAME);
			JobInfo info = jobTracker.submitJob(this);
			
			File jFile = new File(this.jarPath);
			FileInputStream in = new FileInputStream(jFile);
			byte[] temp = new byte[Environment.Dfs.BUF_SIZE];
			int bCount = 0;
			while ((bCount = in.read(temp)) != -1){
				Byte[] data = new Byte[bCount];
				for (int i = 0; i < bCount; i++){
					data[i] = temp[i];
				}
				jobTracker.putJar(info.getID(), this.jarName, data, bCount);
			}
			in.close();

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

		} catch (RemoteException | NotBoundException | InterruptedException | FileNotFoundException e) {

			e.printStackTrace();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}



}
