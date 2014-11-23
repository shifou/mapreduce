package test;

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
	mapTest a =new mapTest("/Users/mlx/fk.txt","/Users/mlx/Downloads/run.jar");
	Thread on = new Thread(a);
	on.run();
	while(on.isAlive())
		{
			System.out.println("running");
			Thread.sleep(2000);
		}
		}
}
