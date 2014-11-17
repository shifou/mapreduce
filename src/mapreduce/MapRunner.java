package mapreduce;

import java.lang.reflect.Constructor;

import mapreduce.io.Context;
import mapreduce.io.Record;
import mapreduce.io.RecordReader;
import mapreduce.io.Text;
import mapreduce.io.TextInputFormat;
import mapreduce.io.Writable;

public class MapRunner<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> implements Runnable{
	public Mapper<K1, V1, K2, V2> mapper;
	public String jobid;
	public String taskid;
	public InputSplit block;
	public int tryNum;
	public Configuration conf;
	public String blockpath;
	public MapRunner(String file,String jid, String tid, InputSplit split, Configuration cf, int trynum)
	{
		blockpath=file;
		jobid=jid;
		taskid=tid;
		tryNum=trynum;
		block=split;
		conf =cf;
	}
	@Override
	public void run() {
		
		Class<Mapper> mapClass;
		try {
			//step1 : get the programmer's Mapper class and Instantiate it
			mapClass = (Class<Mapper>) Class.forName(conf.getMapperClass());
			Constructor<Mapper> constructors = mapClass.getConstructor();
			mapper = constructors.newInstance();
			
			// step2: Get the chunks data, format them using the LineFormat 
			// and filling these into OutputCollector
			String contents[] = new String[numOfChunks];
			int count = 0;
			Context<cong.get,> ct = new MapperOutputCollector();
			ArrayList<String> filePaths = new ArrayList<String>();
			for(KVPair pair : pairLists) {
				Integer chunkNum = (Integer) pair.getKey();
				String sourceNodeIP = (String) pair.getValue();
				Registry reg = LocateRegistry.getRegistry(sourceNodeIP, dataNodeRegPort);
				DataNodeInterface datanode = (DataNodeInterface)reg.lookup(dataNodeService);
				contents[count] = new String(datanode.getFile(jobConf.getInputfile(),chunkNum),"UTF-8");
				Class<InputFormat> inputFormatClass = (Class<InputFormat>) Class.forName(jobConf.getInputFormat().getName());
				Constructor<InputFormat> constuctor = inputFormatClass.getConstructor(String.class);
				InputFormat inputFormat = constuctor.newInstance(contents[count]);
				List<KVPair> kvPairs = inputFormat.getKvPairs();
				for(int i = 0; i < kvPairs.size(); i++) {
					mapper.map(kvPairs.get(i).getKey(), kvPairs.get(i).getValue(), outputCollector);
				}
				count++;
			}
			// step3: partition the OutputCollector
			StringBuffer[] partitionContents = Partitioner.partition(outputCollector.mapperOutputCollector,partitionNums);
			// step4: write the partition contents to the specific path
			for(int j = 0; j < partitionNums; j++) {
				String filename = partitionFilePath + jobID.toString() + "/" + mapperNum.toString() + "/partition" + j;
				filePaths.add(filename);
				IOUtil.writeBinary(partitionContents[j].toString().getBytes("UTF-8"), filename);
			}
			// step5: notify task tracker to update task status
			TaskTracker.updateFilePaths(jobID, filePaths);
			System.out.println("JobID " + jobID + " , mapper number " + mapperNum + " and mapper name " + classname + " has finished!");
			TaskTracker.updateMapStatus(jobID, true);
			
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException 
				| InstantiationException | IllegalAccessException 
				| IllegalArgumentException | InvocationTargetException | NotBoundException | IOException e) {
			try {
				TaskTracker.handleDataNodeFailure(jobID, numOfChunks, jobConf, pairLists,classname,mapperNum, rmiServiceInfo,tryNums);
			} catch (RemoteException e1) {
//				e1.printStackTrace();
			}
			System.err.println("Mapper fails while fetching chunks !!");
			System.exit(-1);
		}
		
	}	
	
}
