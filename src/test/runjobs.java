package test;

import java.util.concurrent.ConcurrentHashMap;

import main.Environment;

public class runjobs {
	public static void main(String[] args) throws InterruptedException {
	try {
		if (Environment.configure() == false) {
			System.out.println("please configure hdfs and mapred first");
			System.exit(1);
		}
	} catch (Exception e) {
		e.printStackTrace();

		System.err.println("please configure hdfs and mapred first");

		System.exit(1);
	}
	// test locally:
	// running 3 map task. 1 task in the slave with servicename tt
	// 					   2 task in the slave with servicename tt2
	// jobid: 2014-11-22-123
	// partitionNum: 3
	mapTest a =new mapTest("/Users/mlx/fk.txt","/Users/mlx/Downloads/run.jar","2014-11-22-123","1","tt",3);
	Thread on = new Thread(a);
	on.run();
	while(on.isAlive())
		{
			System.out.println("running");
			Thread.sleep(2000);
		}
	mapTest b =new mapTest("/Users/mlx/fk2.txt","/Users/mlx/Downloads/run.jar","2014-11-22-123","2","tt2",3);
	on=new Thread(b);
	on.run();
	while(on.isAlive())
	{
		
			System.out.println("running");
			Thread.sleep(2000);
	}
	mapTest c =new mapTest("/Users/mlx/fk3.txt","/Users/mlx/Downloads/run.jar","2014-11-22-123","3","tt",3);
	on=new Thread(c);
	on.run();
	while(on.isAlive())
	{
		
			System.out.println("running");
			Thread.sleep(2000);
	}
	ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> lc =new ConcurrentHashMap<String, ConcurrentHashMap<Integer, String>> ();
	ConcurrentHashMap<Integer,String> hold =new ConcurrentHashMap<Integer,String>();
	hold.put(1, "");
	hold.put(3, "");
	lc.put("tt", hold);
	hold=new ConcurrentHashMap<Integer,String>();
	hold.put(2, "");
	lc.put("tt2", hold);
	reduceTest d =new reduceTest("2014-11-22-123", "0", lc, "tt", "/Users/mlx/Downloads/run.jar", 3);
	on=new Thread(d);
	on.run();
	while(on.isAlive())
	{
		
			System.out.println("running");
			Thread.sleep(2000);
	}
	}
}
