package hdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class HDFSFile implements Serializable{

	private static final long serialVersionUID = 3326499942746127733L;
	public String filename;
	public ConcurrentHashMap<Integer, HDFSBlock> blocks;

	public ConcurrentHashMap<Integer, HDFSBlock> getBlockList() {
		return blocks;
	}

	public int getBlockSize() {
		return blocks.size();
	}

	public void addBlock(Byte[] data, int blockID, int blocksize,
			List<String> locations) {

		HDFSBlock block = new HDFSBlock(this.filename, blockID, data,
				blocksize, locations);

		this.blocks.put(blockID, block);

	}

	public int getBlock(byte[] data, int blockID) {

		
		return -1;
	}

	public HDFSFile(String filename) {
		this.filename = filename;
		this.blocks = new ConcurrentHashMap<Integer, HDFSBlock>();
	}
	@Override
	public String delete() {
		for (Integer one : blocks.keySet()) {
			HDFSBlock hold = blocks.get(one);
			try {
				if(hold.delete()==false)
					System.out.println("notice some nodes fail when delete the block");
			} catch (RemoteException e) {
				System.out.println("delete failed");
				System.exit(-1);
			} catch (NotBoundException e) {
				System.out.println("data node cant find");
				System.exit(-1);
			} catch (IOException e) {
				System.out.println("File Error");
				System.exit(-1);
			}
		}
		return filename;
	}
	public String createFrom( String localFileName)
	{
		int c = 0;
		int blocksize=0;
		InputStream    fis;
		BufferedReader br;
		String         line;
		String temp="";
		fis = new FileInputStream(localFileName);
		try{
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		while ((line = br.readLine()) != null) {
			if(line.getBytes().length>Environment.Dfs.BUF_SIZE)
				return "Abondon some line too big to fit even in a block. ";
		    if((temp+line).getBytes().length <=Environment.Dfs.BUF_SIZE)
		    	temp=temp+line;
		    else
		    {
		    	List<String> locations = NameNode.select(Environment.Dfs.REPLICA_NUMS);
				if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
				{
					return "Abondon put task Reason: can not fulfil replica nums during putting the block\n";
				}
				byte[] buff = temp.getBytes();
				int ct=0;
				Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
				for(byte b: buff)
					   data[ct++] = b;
				file.addBlock(data, blocksize, ct,locations);
		    	temp="";
		    }
		}
		// Done with the file
		br.close();
		br = null;
		fis = null;
	} catch (Exception e) {
		e.printStackTrace();
		return "Error! Failed to put file to HDFS.";
	}
	}
}
