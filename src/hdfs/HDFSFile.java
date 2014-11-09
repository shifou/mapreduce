package hdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class HDFSFile implements Serializable{

	private static final long serialVersionUID = 3326499942746127733L;
	public String filename;
	private String folderName;
	private ConcurrentHashMap<Integer, HDFSBlock> blocks;

	public ConcurrentHashMap<Integer, HDFSBlock> getBlockList() {
		return blocks;
	}

	public int getBlockSize() {
		return blocks.size();
	}

	private void addBlock(Byte[] data, int blockID, int blocksize,
			List<DataNodeInfo> locations) {

		HDFSBlock block = new HDFSBlock(this.filename, blockID, data,
				blocksize, locations, this.folderName);

		this.blocks.put(blockID, block);

	}

	public int getBlock(byte[] data, int blockID) {

		HDFSBlock block = this.blocks.get(blockID);
		if (block != null){
			return block.get(data);
		}
		return -1;
	}

	public HDFSFile(String filename, String folderName) {
		this.filename = filename;
		this.blocks = new ConcurrentHashMap<Integer, HDFSBlock>();
		this.folderName = folderName;
	}
	public String delete() {
		
		for (Integer one : blocks.keySet()) {
			HDFSBlock hold = blocks.get(one);
			if(hold.delete()==false){
				System.out.println("notice some nodes fail when delete the block");
			}
		}
		return filename;
	}
	public String createFrom( String localFileName)
	{
		int blocksize=0;
		InputStream    fis;
		BufferedReader br;
		String         line;
		String temp="";
		try{
			fis = new FileInputStream(localFileName);
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		while ((line = br.readLine()) != null) {
			if(line.getBytes().length>Environment.Dfs.BUF_SIZE){
				br.close();
				return "Abondon some line too big to fit even in a block. ";
			}
				
		    if((temp+line+"\n").getBytes().length <=Environment.Dfs.BUF_SIZE)
		    	temp=temp+line+"\n";
		    else
		    {
		    	List<DataNodeInfo> locations = NameNode.select(Environment.Dfs.REPLICA_NUMS);
				if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
				{
					br.close();
					return "Abondon put task Reason: can not fulfil replica nums during putting the block\n";
				}
				byte[] buff = temp.getBytes();
				int ct=0;
				Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
				for(byte b: buff)
					   data[ct++] = b;
				addBlock(data, blocksize, ct,locations);
				blocksize++;
		    	temp=line;
		    }
		}
		if(temp.equals("")==false)
		{
			List<DataNodeInfo> locations = NameNode.select(Environment.Dfs.REPLICA_NUMS);
			if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
			{
				br.close();
				return "Abondon put task Reason: can not fulfil replica nums during putting the block\n";
			}
			byte[] buff = temp.getBytes();
			int ct=0;
			Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
			for(byte b: buff)
				   data[ct++] = b;
			addBlock(data, blocksize, ct,locations);
			blocksize++;
	    	temp="";
		}
		br.close();
		br = null;
		fis = null;
		return "ok";
	} catch (Exception e) {
		e.printStackTrace();
		return "Error! Failed to put file to HDFS.";
	}
	}

	public String moveTo(String localFilePath) {
		FileOutputStream out = null;
		try {
			out =  new FileOutputStream(localFilePath);
		
		int c;
		int counter = 0;
		byte[] buff = new byte[Environment.Dfs.BUF_SIZE];
		for(int i=0;i<blocks.size();i++)
		{
			c = getBlock(buff,i);
			if(c==-1){
				out.close();
				return "get file failed due to too many node crush and can not get a complete file";
			}
				
			out.write(buff, 0, c);
			counter += c;
		}
		out.close();
		System.out.println("READ: " + counter);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "ok";
	}
}
