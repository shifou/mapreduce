package hdfs;

import java.io.Serializable;

public class HDFSBlock implements Serializable {
	
	private String blockFileName;
	private int ID; 

	/**
	 * 
	 */
	private static final long serialVersionUID = -3110335214705117456L;
	
	public HDFSBlock(String blockFileName, int ID){
		
	}

	public static String getIp() {
		
		return null;
	}

	public static int getPort() {
		
		return 0;
	}

	public static String getServiceName() {
		
		return null;
	}


}
