package hdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class HDFSFile implements Serializable{

	private static final long serialVersionUID = 3326499942746127733L;
	public String filename;
	private String folderName;
	public ConcurrentHashMap<Integer, HDFSBlock> blocks;
	public ConcurrentHashMap<String, Vector<Integer>> slaves2blocklist;  
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
		slaves2blocklist= new ConcurrentHashMap<String, Vector<Integer>>();
	}
	public String delete() {
		String ans="";
		for (Integer one : blocks.keySet()) {
			HDFSBlock hold = blocks.get(one);
			if(hold.delete()==false){
				ans+=("notice some nodes fail when delete the block");
			}
		}
		return ans+"delete ok\n";
	}
	public HashSet<String> createFrom( String localFileName)
	{
		HashSet<String> ans =new HashSet<String>();
		int blocksize=0;
		InputStream    fis;
		BufferedReader br;
		String         line;
		String temp="";
		try{
			fis = new FileInputStream(localFileName);
			if(fis==null)
				System.out.println("--------");
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		while ((line = br.readLine()) != null) {
			if(line.getBytes().length>Environment.Dfs.BUF_SIZE){
				br.close();
				ans.add("#");
				ans.add("#Abondon some line too big to fit even in a block. ");
				return ans;
			}
				
		    if((temp+line+"\n").getBytes().length <=Environment.Dfs.BUF_SIZE)
		    	temp=temp+line+"\n";
		    else
		    {
		    	Vector<DataNodeInfo> locations = NameNode.select(Environment.Dfs.REPLICA_NUMS);
				if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
				{
					br.close();
					ans.add("#");
					ans.add("#Abondon some line too big to fit even in a block. ");
					return ans;
				}	
				for(DataNodeInfo hold:locations)
				{
					ans.add(hold.serviceName);
					if(slaves2blocklist.containsKey(hold.serviceName))
					{
						slaves2blocklist.get(hold.serviceName).add(blocksize);
					}
					else
					{
						Vector<Integer> a= new Vector<Integer>();
						a.add(blocksize);
						slaves2blocklist.put(hold.serviceName,a);
					}
				}
				byte[] buff = temp.getBytes();
				int ct=0;
				Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
				for(byte b: buff)
					   data[ct++] = b;
				addBlock(data, blocksize, ct,locations);
				System.out.println("add block "+blocksize+" of "+filename);
				blocksize++;
		    	temp=line+"\n";
		    }
		}
		if(temp.equals("")==false)
		{
			Vector<DataNodeInfo> locations = NameNode.select(Environment.Dfs.REPLICA_NUMS);
			if(locations.size()!=Environment.Dfs.REPLICA_NUMS)
			{
				br.close();
				ans.add("#");
				ans.add("#Abondon some line too big to fit even in a block. ");
				return ans;
			}
			for(DataNodeInfo hold:locations)
			{
				ans.add(hold.serviceName);
				if(slaves2blocklist.containsKey(hold.serviceName))
				{
					slaves2blocklist.get(hold.serviceName).add(blocksize);
				}
				else
				{
					Vector<Integer> a= new Vector<Integer>();
					a.add(blocksize);
					slaves2blocklist.put(hold.serviceName,a);
				}
			}
			byte[] buff = temp.getBytes();
			int ct=0;
			Byte[]data = new Byte[Environment.Dfs.BUF_SIZE];
			for(byte b: buff)
				   data[ct++] = b;
			addBlock(data, blocksize, ct,locations);
			System.out.println("add block "+blocksize+" of "+filename);
			blocksize++;
	    	temp="";
		}
		br.close();
		br = null;
		fis = null;
		ans.add("?put "+filename+" ok");
		return ans;
	} catch (Exception e) {
		e.printStackTrace();
		ans.add("#");
		ans.add("#Error! Failed to put file to HDFS.");
		return ans;
	}
	}

	public String moveTo(String localFilePath) {
		FileOutputStream out = null;
		try {
			out =  new FileOutputStream(localFilePath+"/"+this.filename);
		
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

	public String Replica(String serviceName) {
		String ans="",res="";
		Vector<Integer> info = slaves2blocklist.get(serviceName);
		for(Integer one:info)
		{
			HDFSBlock temp = blocks.get(one);
			String newServiceName = temp.newReplica(serviceName);
			String[] fk = newServiceName.split("#");
			if(fk.length==2){
				
				if(slaves2blocklist.containsKey(fk[1]))
					slaves2blocklist.get(fk[1]).add(one);
				else
				{
					Vector<Integer> a= new Vector<Integer>();
					a.add(one);
					slaves2blocklist.put(fk[1],a);
				}
				if(res.equals(""))
				res=fk[1];
				else
					res+=(" "+fk[1]);
			}
			ans+=("block: "+one+" "+fk[0]+"\n");
			blocks.put(one, temp);
		}
		slaves2blocklist.remove(serviceName);
		return ans+"#"+res;
	}


}
